import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { S3 } from 'aws-sdk';
import * as iconv from 'iconv-lite';
import { Readable } from 'stream';
import * as readline from 'readline';
import { CreateIndexDto } from './dto/create-index.dto';
import { IndexMetadata, DatabaseStatsResponse } from './types/index.types';

// Custom error class for better error handling
export class PhoneNumberNotFoundException extends NotFoundException {
  constructor(phone: string, databaseId: string | null = null) {
    const message = databaseId
      ? `Телефон ${phone} не найден в базе данных ${databaseId}`
      : `Телефон ${phone} не найден ни в одной базе данных`;
    super(message);
    this.name = 'PhoneNumberNotFoundException';
  }
}

@Injectable()
export class IndexerService {
  private readonly logger = new Logger(IndexerService.name);
  private readonly basePrefix = ''; // Пустой префикс, т.к. нет структуры с префиксами
  private readonly PARTITION_SIZE = 10000;
  private readonly CHUNK_SIZE = 50000;
  private readonly REPORT_INTERVAL = 5000;
  private readonly s3: S3;
  private readonly bucketName: string;
  
  private indexingInProgress = false;
  private processingStats = {
    startTime: 0,
    currentTime: 0,
    totalLines: 0,
    processedLines: 0,
    recordsFound: 0,
    lastReportTime: 0,
    prefixesFound: new Set<string>(),
    bytesProcessed: 0,
    fileSize: 0,
    lastChunkLines: 0,
    linesPerSecond: 0,
    estimatedTimeRemaining: '',
    percentComplete: 0
  };

  constructor() {
    // Используем значения по умолчанию для разработки
    const accessKeyId = process.env.AWS_ACCESS_KEY_ID || '';
    const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY || '';
    
    this.s3 = new S3({
      region: process.env.AWS_REGION || 'eu-west-2',
      credentials: {
        accessKeyId,
        secretAccessKey
      }
    });
    
    this.bucketName = process.env.S3_BUCKET_NAME || 'bdb-indexing';
    this.logger.log(`Инициализация S3IndexerService с бакетом: ${this.bucketName}`);
  }

  private async objectExists(key: string): Promise<boolean> {
    try {
      await this.s3.headObject({
        Bucket: this.bucketName,
        Key: key
      }).promise();
      return true;
    } catch (error) {
      if (error && typeof error === 'object' && 'code' in error && error.code === 'NotFound') {
        return false;
      }
      this.logger.error(`Ошибка при проверке объекта ${key}: ${error.message}`);
      return false;
    }
  }

  private async prefixExists(prefix: string): Promise<boolean> {
    try {
      const response = await this.s3.listObjectsV2({
        Bucket: this.bucketName,
        Prefix: prefix,
        MaxKeys: 1
      }).promise();
      
      return !!(response.Contents && response.Contents.length > 0);
    } catch (error) {
      this.logger.error(`Ошибка при проверке префикса ${prefix}: ${error.message}`);
      throw error;
    }
  }

  private async listFolders(prefix: string = ''): Promise<string[]> {
    try {
      const response = await this.s3.listObjectsV2({
        Bucket: this.bucketName,
        Prefix: prefix,
        Delimiter: '/'
      }).promise();
      
      const folders: string[] = [];
      
      if (response.CommonPrefixes) {
        for (const commonPrefix of response.CommonPrefixes) {
          if (commonPrefix.Prefix) {
            const folderName = commonPrefix.Prefix.slice(prefix.length);
            const cleanName = folderName.replace(/\/$/, '');
            if (cleanName) {
              folders.push(cleanName);
            }
          }
        }
      }
      
      return folders;
    } catch (error) {
      this.logger.error(`Ошибка при получении списка папок ${prefix}: ${error.message}`);
      throw error;
    }
  }

  private async readJsonFile(key: string): Promise<any> {
    try {
      const response = await this.s3.getObject({
        Bucket: this.bucketName,
        Key: key
      }).promise();
      
      if (!response.Body) {
        throw new Error(`Пустое содержимое файла: ${key}`);
      }
      
      const content = response.Body.toString('utf-8');
      
      try {
        // Try regular JSON parsing first
        return JSON.parse(content);
      } catch (jsonError) {
        this.logger.warn(`Ошибка первичного парсинга JSON для файла ${key}: ${jsonError.message}`);
        
        // Attempt repair strategies for common JSON corruption issues
        try {
          // Strategy 1: Find the last valid JSON object by matching the final closing brace
          this.logger.log(`Попытка восстановления JSON данных для файла ${key} (стратегия 1)`);
          const match = /^([^]*})(?:\s*[^]*)?$/s.exec(content);
          
          if (match) {
            const validPart = match[1];
            this.logger.log(`Найден потенциально валидный JSON (длина: ${validPart.length}). Попытка парсинга...`);
            
            try {
              const result = JSON.parse(validPart);
              this.logger.log(`✓ JSON успешно восстановлен (стратегия 1)`);
              return result;
            } catch (repairError) {
              this.logger.warn(`Ошибка восстановления (стратегия 1): ${repairError.message}`);
            }
          }
          
          // Strategy 2: Try manual bracket counting to find the end of the object
          this.logger.log(`Попытка восстановления JSON данных для файла ${key} (стратегия 2)`);
          let braceCount = 0;
          let inString = false;
          let escaped = false;
          let validLength = 0;
          
          for (let i = 0; i < content.length; i++) {
            const char = content[i];
            
            if (inString) {
              if (escaped) {
                escaped = false;
              } else if (char === '\\') {
                escaped = true;
              } else if (char === '"') {
                inString = false;
              }
            } else {
              if (char === '{') braceCount++;
              else if (char === '}') {
                braceCount--;
                if (braceCount === 0) {
                  validLength = i + 1;
                  break;
                }
              } else if (char === '"') {
                inString = true;
              }
            }
          }
          
          if (validLength > 0) {
            const validPart = content.substring(0, validLength);
            this.logger.log(`Найден возможный конец JSON на позиции ${validLength}. Попытка парсинга...`);
            
            try {
              const result = JSON.parse(validPart);
              this.logger.log(`✓ JSON успешно восстановлен (стратегия 2)`);
              return result;
            } catch (repairError) {
              this.logger.warn(`Ошибка восстановления (стратегия 2): ${repairError.message}`);
            }
          }
          
          // If all repair strategies fail
          throw new Error(`Не удалось восстановить поврежденный JSON файл: ${key}`);
        } catch (repairError) {
          this.logger.error(`Все стратегии восстановления JSON не удались: ${repairError.message}`);
          throw jsonError; // Rethrow the original error
        }
      }
    } catch (error) {
      this.logger.error(`Ошибка при чтении JSON из S3 ${key}: ${error.message}`);
      throw error;
    }
  }

  private async writeJsonFile(key: string, data: any): Promise<void> {
    try {
      await this.s3.putObject({
        Bucket: this.bucketName,
        Key: key,
        Body: JSON.stringify(data),
        ContentType: 'application/json'
      }).promise();
    } catch (error) {
      this.logger.error(`Ошибка при записи JSON в S3 ${key}: ${error.message}`);
      throw error;
    }
  }

  private async createFolder(folderPath: string): Promise<void> {
    try {
      await this.s3.putObject({
        Bucket: this.bucketName,
        Key: folderPath.endsWith('/') ? folderPath : folderPath + '/',
        Body: ''
      }).promise();
    } catch (error) {
      this.logger.error(`Ошибка при создании папки ${folderPath}: ${error.message}`);
      throw error;
    }
  }

  private async getObjectSize(key: string): Promise<number> {
    try {
      const response = await this.s3.headObject({
        Bucket: this.bucketName,
        Key: key
      }).promise();
      
      return response.ContentLength || 0;
    } catch (error) {
      this.logger.error(`Ошибка при получении размера объекта ${key}: ${error.message}`);
      throw error;
    }
  }

  async createIndex(filePath: string, dto: CreateIndexDto): Promise<IndexMetadata> {
    if (this.indexingInProgress) {
      this.logger.warn('Индексация уже выполняется. Завершите текущий процесс перед запуском нового.');
      throw new Error('Индексация уже выполняется');
    }
    
    this.indexingInProgress = true;
    this.resetStats();
    
    this.logger.log(`Запуск индексации файла: ${filePath}`);
    this.logger.log(`ID базы данных: ${dto.databaseId}`);
    
    const s3FilePath = filePath.startsWith('/') ? filePath.substring(1) : filePath;
    try {
      const fileSize = await this.getObjectSize(s3FilePath);
      this.processingStats.fileSize = fileSize;
      this.logger.log(`Размер файла: ${this.formatBytes(fileSize)}`);
    } catch (error) {
      this.indexingInProgress = false;
      this.logger.error(`Файл не существует в S3: ${s3FilePath}`);
      throw new Error(`Файл не существует в S3: ${s3FilePath}`);
    }
    
    const encoding = dto.encoding || 'utf8';
    this.logger.log(`Используется кодировка: ${encoding}`);
    
    const databaseFolder = `${dto.databaseId}/`;
    this.logger.log(`Создание директории базы данных: ${databaseFolder}`);
    try {
      await this.createFolder(databaseFolder);
    } catch (error) {
      this.indexingInProgress = false;
      this.logger.error(`Ошибка создания директории: ${error.message}`);
      throw error;
    }

    const metadata: IndexMetadata = {
      id: Date.now().toString(),
      originalFileName: s3FilePath.split('/').pop() || '',
      totalRecords: 0,
      partitionsCount: 0,
      partitionSize: this.PARTITION_SIZE,
      createdAt: new Date(),
      phoneColumn: dto.phoneColumn || 'phone'
    };

    this.processingStats.startTime = Date.now();
    this.processingStats.lastReportTime = Date.now();
    const reportInterval = setInterval(() => this.generateProgressReport(), this.REPORT_INTERVAL);

    let buffer: { [key: string]: any } = {};
    let processed = 0;
    let headers: string[] = [];
    
    return new Promise<IndexMetadata>(async (resolve, reject) => {
      try {
        const s3Object = await this.s3.getObject({
          Bucket: this.bucketName,
          Key: s3FilePath
        }).promise();
        
        if (!s3Object.Body) {
          throw new Error(`Пустое содержимое файла: ${s3FilePath}`);
        }
        
        const fileStream = Readable.from(s3Object.Body as Buffer);
        
        let streamToUse: NodeJS.ReadableStream = fileStream;
        
        if (encoding !== 'utf8') {
          const encodingMap: Record<string, string> = {
            'windows1251': 'win1251',
            'koi8r': 'koi8-r',
            'iso88595': 'iso-8859-5'
          };
          const iconvEncoding = encodingMap[encoding] || encoding;
          streamToUse = fileStream.pipe(iconv.decodeStream(iconvEncoding));
        }
        
        const rl = readline.createInterface({
          input: streamToUse,
          crlfDelay: Infinity
        });

        let isFirstLine = true;

        rl.on('line', async (line) => {
          this.processingStats.processedLines++;
          this.processingStats.bytesProcessed += Buffer.byteLength(line, 'utf8') + 1;
          
          try {
            let rawData = line;
            
            if (line.startsWith('{"') || line.includes('_0')) {
              try {
                const parsedLine = JSON.parse(line);
                rawData = parsedLine._0 || line;
              } catch (e) {
                rawData = line;
              }
            }
            
            const fields = rawData.split('|');
            
            if (isFirstLine) {
              headers = fields.map(h => h.trim());
              isFirstLine = false;
              
              this.logger.log(`Обнаружены заголовки: ${headers.join(', ')}`);
              return;
            }
            
            if (fields.length >= 5) {
              const phoneIndex = 4;
              let phone = phoneIndex < fields.length ? fields[phoneIndex].trim() : null;
              
              if (phone) {
                phone = this.normalizePhone(phone);
                
                if (phone) {
                  const firstDigits = phone.substring(0, 3);
                  this.processingStats.prefixesFound.add(firstDigits);
                  
                  if (!buffer[firstDigits]) {
                    buffer[firstDigits] = {};
                  }
                  
                  buffer[firstDigits][phone] = this.extractPipeDelimitedData(fields, headers);
                  
                  processed++;
                  this.processingStats.recordsFound++;
                  
                  if (processed >= this.CHUNK_SIZE) {
                    await this.saveBufferToS3(databaseFolder, buffer);
                    buffer = {};
                    processed = 0;
                  }
                }
              }
            } else {
              this.logger.debug(`Строка не содержит достаточно полей: ${rawData}`);
            }
          } catch (lineError) {
            this.logger.debug(`Ошибка обработки строки: ${lineError.message}`);
          }
        });

        rl.on('close', async () => {
          clearInterval(reportInterval);
          
          await this.generateProgressReport(true);
          
          if (Object.keys(buffer).length > 0) {
            this.logger.log(`Сохранение оставшихся ${processed} записей`);
            await this.saveBufferToS3(databaseFolder, buffer);
          }
          
          metadata.totalRecords = this.processingStats.recordsFound;
          metadata.partitionsCount = Math.ceil(metadata.totalRecords / this.PARTITION_SIZE);
          
          this.logger.log(`Сохранение метаданных. Всего записей: ${metadata.totalRecords}, партиций: ${metadata.partitionsCount}`);
          await this.saveMetadata(databaseFolder, metadata);
          
          this.logger.log('──────────────────────────────────────────────────────');
          this.logger.log(`✓ Индексация успешно завершена`);
          this.logger.log(`✓ Обработано строк: ${this.processingStats.processedLines.toLocaleString()}`);
          this.logger.log(`✓ Найдено номеров: ${this.processingStats.recordsFound.toLocaleString()}`);
          this.logger.log(`✓ Обработано данных: ${this.formatBytes(this.processingStats.bytesProcessed)}`);
          this.logger.log(`✓ Время обработки: ${this.formatDuration(Date.now() - this.processingStats.startTime)}`);
          this.logger.log('──────────────────────────────────────────────────────');
          
          this.indexingInProgress = false;
          resolve(metadata);
        });

        rl.on('error', (err) => {
          clearInterval(reportInterval);
          this.indexingInProgress = false;
          this.logger.error(`Ошибка чтения файла: ${err.message}`);
          reject(err);
        });
      } catch (error) {
        clearInterval(reportInterval);
        this.indexingInProgress = false;
        this.logger.error(`Неожиданная ошибка в createIndex: ${error.message}`);
        reject(error);
      }
    });
  }

  private normalizePhone(phone: string): string | null {
    if (!phone) return null;
    
    phone = phone.replace(/^["']+|["']+$/g, '');
    
    const digits = phone.replace(/\D/g, '');
    
    if (digits.length >= 10 && digits.length <= 12) {
      if (digits.length === 10) {
        return '7' + digits;
      } else if (digits.length === 11) {
        if (digits[0] === '8') {
          return '7' + digits.substring(1);
        } else if (digits[0] === '7') {
          return digits;
        }
      }
    }
    
    return null;
  }

  private extractPipeDelimitedData(fields: string[], headers: string[]): any {
    const record: any = {};
    
    for (let i = 0; i < Math.min(fields.length, headers.length); i++) {
      const header = headers[i];
      const value = fields[i]?.trim() || '';
      record[header] = value;
      
      record[`_${i}`] = value;
    }
    
    record.lastName = fields[0]?.trim() || '';
    record.firstName = fields[1]?.trim() || '';
    record.middleName = fields[2]?.trim() || '';
    record.fullName = `${record.lastName} ${record.firstName} ${record.middleName}`.trim();
    record.birthDate = fields[3]?.trim() || '';
    
    const phoneRaw = fields[4]?.trim() || '';
    const normalizedPhone = this.normalizePhone(phoneRaw) || '';
    record.phone = normalizedPhone;
    record.formattedPhone = this.formatPhoneNumber(normalizedPhone);
    
    if (headers.length > 5) record.snils = fields[5]?.trim() || '';
    if (headers.length > 6) record.inn = fields[6]?.trim() || '';
    if (headers.length > 7) record.email = fields[7]?.trim() || '';
    
    return record;
  }

  private resetStats(): void {
    this.processingStats = {
      startTime: 0,
      currentTime: 0,
      totalLines: 0,
      processedLines: 0,
      recordsFound: 0,
      lastReportTime: 0,
      prefixesFound: new Set<string>(),
      bytesProcessed: 0,
      fileSize: 0,
      lastChunkLines: 0,
      linesPerSecond: 0,
      estimatedTimeRemaining: '',
      percentComplete: 0
    };
  }

  private async generateProgressReport(isFinal = false): Promise<void> {
    const now = Date.now();
    const elapsedSinceLastReport = now - this.processingStats.lastReportTime;
    
    if (elapsedSinceLastReport < this.REPORT_INTERVAL && !isFinal) {
      return;
    }
    
    this.processingStats.currentTime = now;
    
    const linesProcessedSinceLastReport = this.processingStats.processedLines - this.processingStats.lastChunkLines;
    this.processingStats.linesPerSecond = Math.round(linesProcessedSinceLastReport / (elapsedSinceLastReport / 1000));
    
    if (this.processingStats.fileSize > 0) {
      this.processingStats.percentComplete = Math.min(
        100, 
        Math.round((this.processingStats.bytesProcessed / this.processingStats.fileSize) * 100)
      );
    }
    
    if (this.processingStats.linesPerSecond > 0 && this.processingStats.fileSize > 0 && this.processingStats.bytesProcessed > 0) {
      const bytesRemaining = this.processingStats.fileSize - this.processingStats.bytesProcessed;
      const bytesPerSecond = this.processingStats.bytesProcessed / ((now - this.processingStats.startTime) / 1000);
      const secondsRemaining = Math.round(bytesRemaining / bytesPerSecond);
      this.processingStats.estimatedTimeRemaining = this.formatDuration(secondsRemaining * 1000);
    } else {
      this.processingStats.estimatedTimeRemaining = 'расчет...';
    }
    
    this.logger.log('────────────────── ОТЧЕТ О ПРОГРЕССЕ ──────────────────');
    this.logger.log(`Прогресс: ${this.processingStats.percentComplete}%`);
    this.logger.log(`Обработано строк: ${this.processingStats.processedLines.toLocaleString()}`);
    this.logger.log(`Найдено номеров: ${this.processingStats.recordsFound.toLocaleString()}`);
    this.logger.log(`Найдено префиксов: ${this.processingStats.prefixesFound.size}`);
    this.logger.log(`Скорость: ${this.processingStats.linesPerSecond.toLocaleString()} строк/сек`);
    this.logger.log(`Обработано: ${this.formatBytes(this.processingStats.bytesProcessed)} из ${this.formatBytes(this.processingStats.fileSize)}`);
    this.logger.log(`Прошло времени: ${this.formatDuration(now - this.processingStats.startTime)}`);
    this.logger.log(`Осталось времени: ${this.processingStats.estimatedTimeRemaining}`);
    this.logger.log('──────────────────────────────────────────────────────');
    
    this.processingStats.lastReportTime = now;
    this.processingStats.lastChunkLines = this.processingStats.processedLines;
  }

  private async saveBufferToS3(databaseFolder: string, buffer: { [key: string]: any }): Promise<void> {
    const prefixCount = Object.keys(buffer).length;
    
    for (const prefix of Object.keys(buffer)) {
      const prefixPath = `${databaseFolder}${prefix}/`;
      
      try {
        await this.createFolder(prefixPath);
        
        const dataKey = `${prefixPath}data.json`;
        await this.writeJsonFile(dataKey, buffer[prefix]);
      } catch (error) {
        this.logger.error(`Ошибка сохранения данных для префикса ${prefix}: ${error.message}`);
      }
    }
  }

  private async saveMetadata(databaseFolder: string, metadata: IndexMetadata): Promise<void> {
    try {
      const metadataKey = `${databaseFolder}metadata.json`;
      await this.writeJsonFile(metadataKey, metadata);
    } catch (error) {
      this.logger.error(`Ошибка сохранения метаданных: ${error.message}`);
      throw error;
    }
  }

  private formatPhoneNumber(phone: string): string {
    if (phone.length !== 11) return phone;
    return `+${phone[0]} (${phone.substring(1, 4)}) ${phone.substring(4, 7)}-${phone.substring(7, 9)}-${phone.substring(9, 11)}`;
  }

  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 Байт';
    
    const sizes = ['Байт', 'КБ', 'МБ', 'ГБ', 'ТБ'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    
    return parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + ' ' + sizes[i];
  }

  private formatDuration(ms: number): string {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    
    const remainingMinutes = minutes % 60;
    const remainingSeconds = seconds % 60;
    
    let result = '';
    
    if (hours > 0) {
      result += `${hours} ч `;
    }
    
    if (hours > 0 || remainingMinutes > 0) {
      result += `${remainingMinutes} мин `;
    }
    
    result += `${remainingSeconds} сек`;
    
    return result;
  }
  
  async getIndexingStatus(): Promise<any> {
    if (!this.indexingInProgress) {
      return {
        status: 'idle',
        message: 'Нет активного процесса индексации'
      };
    }
    
    return {
      status: 'processing',
      progress: {
        percentComplete: this.processingStats.percentComplete,
        processedLines: this.processingStats.processedLines,
        recordsFound: this.processingStats.recordsFound,
        linesPerSecond: this.processingStats.linesPerSecond,
        elapsedTime: this.formatDuration(Date.now() - this.processingStats.startTime),
        estimatedTimeRemaining: this.processingStats.estimatedTimeRemaining,
        bytesProcessed: this.formatBytes(this.processingStats.bytesProcessed),
        totalSize: this.formatBytes(this.processingStats.fileSize)
      }
    };
  }

  async findByPhone(databaseId: string, phone: string): Promise<any> {
    this.logger.log(`===== 🔍 ДЕТАЛЬНЫЙ ПОИСК ПО ТЕЛЕФОНУ =====`);
    this.logger.log(`Запрос поиска по телефону: "${phone}" в базе: "${databaseId}"`);
    
    try {
      const dbFolder = `${databaseId}/`;
      this.logger.log(`Проверка директории базы данных: "${dbFolder}"`);
      
      const dbExists = await this.prefixExists(dbFolder);
      if (!dbExists) {
        this.logger.warn(`⚠️ Директория базы данных не существует: "${dbFolder}"`);
        throw new NotFoundException(`База данных ${databaseId} не найдена`);
      } else {
        this.logger.log(`✓ База данных существует: "${dbFolder}"`);
      }

      let normalizedPhone = '';
      if (phone) {
        normalizedPhone = phone.replace(/\D/g, '');
        this.logger.log(`Телефон после удаления нецифровых символов: "${normalizedPhone}"`);
        
        if (normalizedPhone.startsWith('8') && normalizedPhone.length === 11) {
          normalizedPhone = '7' + normalizedPhone.substring(1);
          this.logger.log(`Телефон нормализован с 8 на 7: "${normalizedPhone}"`);
        }
        
        if (normalizedPhone.length === 10 && normalizedPhone[0] === '9') {
          normalizedPhone = '7' + normalizedPhone;
          this.logger.log(`Телефон дополнен 7 в начале: "${normalizedPhone}"`);
        }
      }
      
      if (!normalizedPhone || normalizedPhone.length !== 11 || normalizedPhone[0] !== '7') {
        this.logger.warn(`⚠️ Неверный формат телефона после нормализации: "${normalizedPhone}"`);
        throw new NotFoundException(`Неверный формат номера телефона: ${phone}`);
      }
      
      this.logger.log(`✓ Нормализованный телефон: "${normalizedPhone}"`);

      const prefix = normalizedPhone.substring(0, 3);
      this.logger.log(`Извлечен префикс телефона: "${prefix}"`);
      
      const prefixPath = `${databaseId}/${prefix}/`;
      this.logger.log(`Проверка директории префикса: "${prefixPath}"`);
      
      const prefixExists = await this.prefixExists(prefixPath);
      if (!prefixExists) {
        this.logger.warn(`⚠️ Директория префикса не существует: "${prefixPath}"`);
        
        try {
          const availablePrefixes = await this.listFolders(`${databaseId}/`);
          if (availablePrefixes.length > 0) {
            this.logger.log(`ℹ️ Доступные префиксы в базе "${databaseId}": ${availablePrefixes.join(', ')}`);
          } else {
            this.logger.log(`ℹ️ В базе "${databaseId}" нет префиксов`);
          }
        } catch (err) {
          this.logger.error(`❌ Ошибка при получении списка префиксов: ${err.message}`);
        }
        
        throw new PhoneNumberNotFoundException(phone, databaseId);
      } else {
        this.logger.log(`✓ Директория префикса существует: "${prefixPath}"`);
      }

      const dataPath = `${databaseId}/${prefix}/data.json`;
      this.logger.log(`Проверка файла данных: "${dataPath}"`);
      
      try {
        const data = await this.readJsonFile(dataPath);
        this.logger.log(`✓ Файл данных успешно прочитан, количество записей: ${Object.keys(data).length}`);
        
        const sampleKeys = Object.keys(data).slice(0, 5);
        this.logger.log(`ℹ️ Примеры ключей: ${sampleKeys.join(', ')}`);
        
        this.logger.log(`🔎 Ищем ключ "${normalizedPhone}" в данных...`);
        const record = data[normalizedPhone];
        if (record) {
          this.logger.log(`✅ Запись найдена для телефона: "${normalizedPhone}"`);
          return record;
        } else {
          this.logger.warn(`❌ Запись не найдена для телефона: "${normalizedPhone}"`);
          
          const similarPhones = Object.keys(data).filter(key => 
            key.startsWith(normalizedPhone.substring(0, 8)) || 
            normalizedPhone.startsWith(key.substring(0, 8))
          );
          
          if (similarPhones.length > 0) {
            this.logger.log(`ℹ️ Найдены похожие телефоны: ${similarPhones.join(', ')}`);
          }
          
          throw new PhoneNumberNotFoundException(phone, databaseId);
        }
      } catch (error) {
        if (error instanceof PhoneNumberNotFoundException) {
          throw error;
        }
        this.logger.error(`❌ Ошибка при чтении/парсинге файла ${dataPath}: ${error.message}`);
        throw new NotFoundException(`Ошибка доступа к данным в базе ${databaseId}`);
      }
    } catch (error) {
      if (error instanceof NotFoundException || error instanceof PhoneNumberNotFoundException) {
        throw error;
      }
      this.logger.error(`❌ Неожиданная ошибка в findByPhone: ${error.message}`);
      throw new NotFoundException(`Ошибка при поиске телефона ${phone} в базе ${databaseId}`);
    } finally {
      this.logger.log(`===== 🏁 ПОИСК ПО ТЕЛЕФОНУ ЗАВЕРШЁН =====`);
    }
  }

  async findByPhoneParallel(phone: string): Promise<any> {
    try {
      const databases = await this.getAllDatabases();
      if (databases.length === 0) {
        this.logger.warn(`⚠️ Не найдено баз данных для поиска`);
        throw new NotFoundException('Не найдено баз данных для поиска');
      }
      
      const batchSize = 3;
      const results: (any | null)[] = [];
      const errors: Error[] = [];
      
      this.logger.log(`🔄 Параллельный поиск по телефону "${phone}" в ${databases.length} базах`);
      
      for (let i = 0; i < databases.length; i += batchSize) {
        const batch = databases.slice(i, i + batchSize);
        this.logger.log(`ℹ️ Поиск в пакете баз ${i/batchSize + 1}/${Math.ceil(databases.length/batchSize)}: ${batch.join(', ')}`);
        
        const batchPromises = batch.map(async (databaseId) => {
          try {
            await new Promise(resolve => setTimeout(resolve, 500));
            
            const result = await this.findByPhone(databaseId, phone)
              .catch(error => {
                if (error instanceof PhoneNumberNotFoundException) {
                  return null;
                }
                errors.push(error);
                return null;
              });
              
            if (result) {
              this.logger.log(`✅ Найден результат в базе "${databaseId}"`);
              return { database: databaseId, ...result };
            }
          } catch (error) {
            this.logger.error(`❌ Ошибка поиска в базе ${databaseId}: ${error.message}`);
            errors.push(error);
          }
          return null;
        });

        const batchResults = await Promise.all(batchPromises);
        results.push(...batchResults.filter(Boolean));

        if (results.length > 0) {
          this.logger.log(`🎯 Найден результат, прерываем поиск`);
          break;
        }
      }

      if (results.length === 0) {
        this.logger.log(`❌ Результатов не найдено`);
        throw new PhoneNumberNotFoundException(phone);
      }
      
      return results[0];
    } catch (error) {
      if (error instanceof NotFoundException || error instanceof PhoneNumberNotFoundException) {
        throw error;
      }
      this.logger.error(`❌ Ошибка в параллельном поиске: ${error.message}`);
      throw new NotFoundException(`Ошибка при параллельном поиске телефона ${phone}`);
    }
  }

  async findByPhoneWithProgress(
    phone: string, 
    progressCallback: (progress: {
      currentDatabase: string;
      progress: number;
      searching: boolean;
      found: boolean;
      result?: any;
      isComplete: boolean;
      totalDatabases?: number;
      currentDatabaseIndex?: number;
      error?: string;
    }) => void
  ): Promise<void> {
    try {
      const databases = await this.getAllDatabases();
      
      if (databases.length === 0) {
        this.logger.log(`ℹ️ Нет доступных баз данных для поиска`);
        progressCallback({
          currentDatabase: '',
          progress: 100,
          searching: false,
          found: false,
          isComplete: true,
          totalDatabases: 0,
          currentDatabaseIndex: 0,
          error: 'Нет доступных баз данных для поиска'
        });
        return;
      }
      
      const total = databases.length;
      this.logger.log(`🔍 Поиск с прогрессом по телефону "${phone}" в ${total} базах данных`);

      progressCallback({
        currentDatabase: databases[0],
        progress: 0,
        searching: true,
        found: false,
        isComplete: false,
        totalDatabases: total,
        currentDatabaseIndex: 0
      });

      for (let i = 0; i < databases.length; i++) {
        const databaseId = databases[i];
        const currentProgress = Math.round((i / total) * 100);
        this.logger.log(`🔄 Поиск в базе "${databaseId}" (${i+1}/${total}, ${currentProgress}%)`);

        progressCallback({
          currentDatabase: databaseId,
          progress: currentProgress,
          searching: true,
          found: false,
          isComplete: false,
          totalDatabases: total,
          currentDatabaseIndex: i
        });

        await new Promise(resolve => setTimeout(resolve, 500));

        try {
          const result = await this.findByPhone(databaseId, phone)
            .catch(error => {
              if (error instanceof PhoneNumberNotFoundException) {
                return null;
              }
              throw error;
            });
            
          if (result) {
            this.logger.log(`✅ Запись найдена в базе "${databaseId}"`);
            progressCallback({
              currentDatabase: databaseId,
              progress: 100,
              searching: false,
              found: true,
              result: { database: databaseId, ...result },
              isComplete: true,
              totalDatabases: total,
              currentDatabaseIndex: i + 1
            });
            return;
          }
        } catch (error) {
          this.logger.error(`❌ Ошибка поиска в базе ${databaseId}: ${error.message}`);
          progressCallback({
            currentDatabase: databaseId,
            progress: currentProgress,
            searching: false,
            found: false,
            isComplete: false,
            totalDatabases: total,
            currentDatabaseIndex: i,
            error: `Ошибка поиска в базе ${databaseId}: ${error.message}`
          });
        }
      }

      this.logger.log(`❌ Запись не найдена во всех базах данных`);
      progressCallback({
        currentDatabase: databases[databases.length - 1],
        progress: 100,
        searching: false,
        found: false,
        result: null,
        isComplete: true,
        totalDatabases: total,
        currentDatabaseIndex: total,
        error: `Телефон ${phone} не найден ни в одной базе данных`
      });

    } catch (error) {
      this.logger.error(`❌ Ошибка в поиске с прогрессом: ${error.message}`);
      progressCallback({
        currentDatabase: '',
        progress: 100,
        searching: false,
        found: false,
        result: null,
        isComplete: true,
        totalDatabases: 0,
        currentDatabaseIndex: 0,
        error: `Ошибка поиска: ${error.message}`
      });
    }
  }

  async getAllDatabases(): Promise<string[]> {
    try {
      const folders = await this.listFolders();
      this.logger.log(`🔍 Получение списка всех баз данных из корня бакета`);
      this.logger.log(`✅ Найдено ${folders.length} баз данных: ${folders.join(', ')}`);
      
      return folders;
    } catch (error) {
      this.logger.error(`❌ Ошибка получения списка баз данных: ${error.message}`);
      return [];
    }
  }

  async getDatabaseStats(): Promise<DatabaseStatsResponse> {
    try {
      const databases = await this.getAllDatabases();
      this.logger.log(`🔍 Получение статистики для ${databases.length} баз данных`);
      
      const stats: DatabaseStatsResponse = {
        totalDatabases: databases.length,
        totalRecords: 0,
        databases: []
      };

      for (const dbId of databases) {
        try {
          this.logger.log(`📊 Получение статистики для базы "${dbId}"`);
          const dbStats = await this.getSingleDatabaseStats(dbId);
          stats.totalRecords += dbStats.totalRecords;
          stats.databases.push({
            id: dbId,
            records: dbStats.totalRecords,
            partitions: dbStats.partitions
          });
          this.logger.log(`✅ Статистика базы "${dbId}": ${dbStats.totalRecords} записей, ${dbStats.partitions} партиций`);
        } catch (error) {
          this.logger.error(`❌ Ошибка получения статистики для базы ${dbId}: ${error.message}`);
          stats.databases.push({
            id: dbId,
            records: 0,
            partitions: 0
          });
        }
      }

      this.logger.log(`📈 Общая статистика: ${stats.totalDatabases} баз данных, ${stats.totalRecords} записей`);
      return stats;
    } catch (error) {
      this.logger.error(`❌ Ошибка получения статистики баз данных: ${error.message}`);
      throw error;
    }
  }

  async getSingleDatabaseStats(databaseId: string): Promise<{
    databaseId: string;
    totalRecords: number;
    partitions: number;
    prefixes: string[];
    createdAt: Date;
  }> {
    try {
      const dbFolder = `${databaseId}/`;
      this.logger.log(`🔍 Получение статистики для базы "${databaseId}"`);
      
      const dbExists = await this.prefixExists(dbFolder);
      if (!dbExists) {
        this.logger.warn(`⚠️ Директория базы данных не существует: ${dbFolder}`);
        throw new Error(`Директория базы данных не существует: ${dbFolder}`);
      }
      
      const metadataKey = `${dbFolder}metadata.json`;
      this.logger.log(`📖 Чтение метаданных из файла "${metadataKey}"`);
      
      let metadata: any = {};
      try {
        metadata = await this.readJsonFile(metadataKey);
      } catch (error) {
        this.logger.error(`❌ Ошибка чтения метаданных: ${error.message}`);
        throw new Error(`Файл метаданных не существует или поврежден: ${metadataKey}`);
      }
      
      this.logger.log(`📁 Получение списка префиксов в базе "${databaseId}"`);
      const prefixes = await this.listFolders(dbFolder);
      this.logger.log(`✅ Найдено ${prefixes.length} префиксов`);

      return {
        databaseId,
        totalRecords: metadata.totalRecords || 0,
        partitions: metadata.partitionsCount || 0,
        prefixes,
        createdAt: new Date(metadata.createdAt)
      };
    } catch (error) {
      this.logger.error(`❌ Ошибка получения статистики для базы ${databaseId}: ${error.message}`);
      throw error;
    }
  }
}