import { Injectable, Logger } from '@nestjs/common';
import * as fs from 'fs/promises';
import { statSync, createReadStream, existsSync, mkdirSync } from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import * as iconv from 'iconv-lite';
import { CreateIndexDto } from './dto/create-index.dto';
import { IndexMetadata, DatabaseStatsResponse } from './types/index.types';

@Injectable()
export class IndexerService {
  private readonly logger = new Logger(IndexerService.name);
  private readonly baseDir = './uploads';
  private readonly PARTITION_SIZE = 10000; // Увеличенный размер партиции для лучшей производительности
  private readonly CHUNK_SIZE = 50000; // Увеличенный размер порции для уменьшения операций с диском
  private readonly REPORT_INTERVAL = 5000; // Интервал отчетов в миллисекундах (5 секунд)
  
  // Флаги состояния для отслеживания прогресса
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
    // Создаем базовую директорию, если она не существует
    if (!existsSync(this.baseDir)) {
      mkdirSync(this.baseDir, { recursive: true });
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
    
    // Проверяем существование файла и получаем его размер
    try {
      const stats = statSync(filePath);
      this.processingStats.fileSize = stats.size;
      this.logger.log(`Размер файла: ${this.formatBytes(stats.size)}`);
    } catch (error) {
      this.indexingInProgress = false;
      this.logger.error(`Файл не существует: ${filePath}`);
      throw new Error(`Файл не существует: ${filePath}`);
    }
    
    // Определяем кодировку файла (используем указанную в параметрах или utf8 по умолчанию)
    const encoding = dto.encoding || 'utf8';
    this.logger.log(`Используется кодировка: ${encoding}`);
    
    // Создаем директорию для базы данных
    const databaseDir = path.join(this.baseDir, dto.databaseId);
    this.logger.log(`Создание директории базы данных: ${databaseDir}`);
    try {
      await fs.mkdir(databaseDir, { recursive: true });
    } catch (error) {
      this.indexingInProgress = false;
      this.logger.error(`Ошибка создания директории: ${error.message}`);
      throw error;
    }

    const metadata: IndexMetadata = {
      id: Date.now().toString(),
      originalFileName: path.basename(filePath),
      totalRecords: 0,
      partitionsCount: 0,
      partitionSize: this.PARTITION_SIZE,
      createdAt: new Date(),
      phoneColumn: dto.phoneColumn || 'phone'
    };

    // Запустим таймер для периодических отчетов
    this.processingStats.startTime = Date.now();
    this.processingStats.lastReportTime = Date.now();
    const reportInterval = setInterval(() => this.generateProgressReport(), this.REPORT_INTERVAL);

    let buffer: { [key: string]: any } = {};
    let processed = 0;
    let headers: string[] = [];
    
    return new Promise<IndexMetadata>((resolve, reject) => {
      try {
        // Создаем поток чтения с поддержкой кодировки
        const fileStream = createReadStream(filePath, { 
          highWaterMark: 64 * 1024 // Увеличиваем размер буфера для быстрого чтения
        });
        
        // Используем iconv для конвертации кодировки, если нужно
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

        rl.on('line', (line) => {
          this.processingStats.processedLines++;
          this.processingStats.bytesProcessed += Buffer.byteLength(line, 'utf8') + 1; // +1 для символа новой строки
          
          try {
            // Извлекаем строку из поля "_0" (так как все данные находятся в этом поле)
            let rawData = line;
            
            // Если строка начинается с кавычек или содержит другие специальные символы JSON
            if (line.startsWith('{"') || line.includes('_0')) {
              try {
                const parsedLine = JSON.parse(line);
                rawData = parsedLine._0 || line;
              } catch (e) {
                // Если не удалось разобрать JSON, используем строку как есть
                rawData = line;
              }
            }
            
            // Разбиваем строку на поля по разделителю "|"
            const fields = rawData.split('|');
            
            // Обрабатываем заголовки в первой строке
            if (isFirstLine) {
              headers = fields.map(h => h.trim());
              isFirstLine = false;
              
              this.logger.log(`Обнаружены заголовки: ${headers.join(', ')}`);
              return;
            }
            
            // Проверяем, что у нас достаточно полей
            if (fields.length >= 5) { // Минимум 5 полей для обработки (включая телефон)
              // Извлекаем телефон (5-е поле по счету, индекс 4)
              const phoneIndex = 4; // Индекс поля с телефоном
              let phone = phoneIndex < fields.length ? fields[phoneIndex].trim() : null;
              
              if (phone) {
                phone = this.normalizePhone(phone);
                
                if (phone) {
                  const firstDigits = phone.substring(0, 3); // Группируем по первым цифрам номера
                  this.processingStats.prefixesFound.add(firstDigits);
                  
                  if (!buffer[firstDigits]) {
                    buffer[firstDigits] = {};
                  }
                  
                  // Создаем запись с указанием полей по заголовкам
                  buffer[firstDigits][phone] = this.extractPipeDelimitedData(fields, headers);
                  
                  processed++;
                  this.processingStats.recordsFound++;
                  
                  // Сохраняем когда накопилось достаточно записей
                  if (processed >= this.CHUNK_SIZE) {
                    this.saveBufferToDisk(databaseDir, buffer);
                    buffer = {};
                    processed = 0;
                  }
                }
              }
            } else {
              this.logger.debug(`Строка не содержит достаточно полей: ${rawData}`);
            }
          } catch (lineError) {
            // Продолжаем работу даже при ошибке в одной строке
            // Но записываем ошибку в лог
            this.logger.debug(`Ошибка обработки строки: ${lineError.message}`);
          }
        });

        rl.on('close', async () => {
          // Остановка таймера отчетов
          clearInterval(reportInterval);
          
          // Финальный отчет
          this.generateProgressReport(true);
          
          if (Object.keys(buffer).length > 0) {
            this.logger.log(`Сохранение оставшихся ${processed} записей`);
            await this.saveBufferToDisk(databaseDir, buffer);
          }
          
          metadata.totalRecords = this.processingStats.recordsFound;
          metadata.partitionsCount = Math.ceil(metadata.totalRecords / this.PARTITION_SIZE);
          
          this.logger.log(`Сохранение метаданных. Всего записей: ${metadata.totalRecords}, партиций: ${metadata.partitionsCount}`);
          await this.saveMetadata(databaseDir, metadata);
          
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

  // Нормализация телефонного номера
  private normalizePhone(phone: string): string | null {
    if (!phone) return null;
    
    // Удаляем лишние кавычки, если они есть
    phone = phone.replace(/^["']+|["']+$/g, '');
    
    // Извлекаем только цифры
    const digits = phone.replace(/\D/g, '');
    
    // Проверка на валидную длину
    if (digits.length >= 10 && digits.length <= 12) {
      // Нормализация
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

  // Извлечение данных из строки с разделителем "|"
  private extractPipeDelimitedData(fields: string[], headers: string[]): any {
    const record: any = {};
    
    // Заполняем все доступные поля по заголовкам
    for (let i = 0; i < Math.min(fields.length, headers.length); i++) {
      const header = headers[i];
      const value = fields[i]?.trim() || '';
      record[header] = value;
      
      // Добавляем оригинальные индексированные поля для совместимости
      record[`_${i}`] = value;
    }
    
    // Добавляем обработанные основные поля
    record.lastName = fields[0]?.trim() || '';
    record.firstName = fields[1]?.trim() || '';
    record.middleName = fields[2]?.trim() || '';
    record.fullName = `${record.lastName} ${record.firstName} ${record.middleName}`.trim();
    record.birthDate = fields[3]?.trim() || '';
    
    // Обрабатываем телефон
    const phoneRaw = fields[4]?.trim() || '';
    const normalizedPhone = this.normalizePhone(phoneRaw) || '';
    record.phone = normalizedPhone;
    record.formattedPhone = this.formatPhoneNumber(normalizedPhone);
    
    // Добавляем дополнительные поля
    if (headers.length > 5) record.snils = fields[5]?.trim() || '';
    if (headers.length > 6) record.inn = fields[6]?.trim() || '';
    if (headers.length > 7) record.email = fields[7]?.trim() || '';
    
    return record;
  }

  // Сброс статистики перед началом новой индексации
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

  // Генерация периодического отчета о прогрессе
  private generateProgressReport(isFinal = false): void {
    const now = Date.now();
    const elapsedSinceLastReport = now - this.processingStats.lastReportTime;
    
    if (elapsedSinceLastReport < this.REPORT_INTERVAL && !isFinal) {
      return; // Отчет еще не нужен
    }
    
    // Обновляем текущее время
    this.processingStats.currentTime = now;
    
    // Рассчитываем скорость обработки
    const linesProcessedSinceLastReport = this.processingStats.processedLines - this.processingStats.lastChunkLines;
    this.processingStats.linesPerSecond = Math.round(linesProcessedSinceLastReport / (elapsedSinceLastReport / 1000));
    
    // Рассчитываем процент выполнения (если известен размер файла)
    if (this.processingStats.fileSize > 0) {
      this.processingStats.percentComplete = Math.min(
        100, 
        Math.round((this.processingStats.bytesProcessed / this.processingStats.fileSize) * 100)
      );
    }
    
    // Рассчитываем оставшееся время
    if (this.processingStats.linesPerSecond > 0 && this.processingStats.fileSize > 0 && this.processingStats.bytesProcessed > 0) {
      const bytesRemaining = this.processingStats.fileSize - this.processingStats.bytesProcessed;
      const bytesPerSecond = this.processingStats.bytesProcessed / ((now - this.processingStats.startTime) / 1000);
      const secondsRemaining = Math.round(bytesRemaining / bytesPerSecond);
      this.processingStats.estimatedTimeRemaining = this.formatDuration(secondsRemaining * 1000);
    } else {
      this.processingStats.estimatedTimeRemaining = 'расчет...';
    }
    
    // Выводим отчет
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
    
    // Сохраняем значения для следующего отчета
    this.processingStats.lastReportTime = now;
    this.processingStats.lastChunkLines = this.processingStats.processedLines;
  }

  // Сохранение буфера на диск
  private async saveBufferToDisk(databaseDir: string, buffer: { [key: string]: any }): Promise<void> {
    const prefixCount = Object.keys(buffer).length;
    
    for (const prefix of Object.keys(buffer)) {
      const prefixDir = path.join(databaseDir, prefix);
      
      try {
        // Используем синхронную версию для уменьшения накладных расходов
        if (!existsSync(prefixDir)) {
          mkdirSync(prefixDir, { recursive: true });
        }
        
        const dataPath = path.join(prefixDir, 'data.json');
        await fs.writeFile(dataPath, JSON.stringify(buffer[prefix]));
      } catch (error) {
        this.logger.error(`Ошибка сохранения данных для префикса ${prefix}: ${error.message}`);
      }
    }
  }

  // Сохранение метаданных
  private async saveMetadata(databaseDir: string, metadata: IndexMetadata): Promise<void> {
    try {
      const metadataPath = path.join(databaseDir, 'metadata.json');
      await fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2));
    } catch (error) {
      this.logger.error(`Ошибка сохранения метаданных: ${error.message}`);
      throw error;
    }
  }

  // Форматирование телефонного номера для отображения
  private formatPhoneNumber(phone: string): string {
    if (phone.length !== 11) return phone;
    return `+${phone[0]} (${phone.substring(1, 4)}) ${phone.substring(4, 7)}-${phone.substring(7, 9)}-${phone.substring(9, 11)}`;
  }

  // Форматирование байтов в человекочитаемый формат
  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 Байт';
    
    const sizes = ['Байт', 'КБ', 'МБ', 'ГБ', 'ТБ'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    
    return parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + ' ' + sizes[i];
  }

  // Форматирование длительности в человекочитаемый формат
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
  
  // Метод для получения статуса текущей индексации
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

  // Полностью переработанный метод поиска по телефону
  async findByPhone(databaseId: string, phone: string): Promise<any | null> {
    this.logger.debug(`Search requested for phone: ${phone} in database: ${databaseId}`);
    
    try {
      // 1. Проверяем существование базы данных
      const dbDir = path.join(this.baseDir, databaseId);
      this.logger.debug(`Checking database directory: ${dbDir}`);
      
      try {
        const dbStat = await fs.stat(dbDir);
        if (!dbStat.isDirectory()) {
          this.logger.warn(`Database path exists but is not a directory: ${dbDir}`);
          return null;
        }
      } catch (error) {
        this.logger.warn(`Database directory does not exist: ${dbDir} - ${error.message}`);
        return null;
      }

      // 2. Нормализуем телефон для поиска
      let normalizedPhone = '';
      if (phone) {
        normalizedPhone = phone.replace(/\D/g, '');
        
        if (normalizedPhone.startsWith('8') && normalizedPhone.length === 11) {
          normalizedPhone = '7' + normalizedPhone.substring(1);
        }
        
        if (normalizedPhone.length === 10 && normalizedPhone[0] === '9') {
          normalizedPhone = '7' + normalizedPhone;
        }
      }
      
      if (!normalizedPhone || normalizedPhone.length !== 11 || normalizedPhone[0] !== '7') {
        this.logger.warn(`Invalid phone number format after normalization: ${normalizedPhone}`);
        return null;
      }
      
      this.logger.debug(`Normalized phone: ${normalizedPhone}`);

      // 3. Определяем каталог префикса
      const prefix = normalizedPhone.substring(0, 3);
      const prefixDir = path.join(dbDir, prefix);
      this.logger.debug(`Checking prefix directory: ${prefixDir}`);
      
      try {
        const prefixStat = await fs.stat(prefixDir);
        if (!prefixStat.isDirectory()) {
          this.logger.warn(`Prefix path exists but is not a directory: ${prefixDir}`);
          return null;
        }
      } catch (error) {
        this.logger.warn(`Prefix directory does not exist: ${prefixDir} - ${error.message}`);
        return null;
      }

      // 4. Проверяем файл данных
      const dataPath = path.join(prefixDir, 'data.json');
      this.logger.debug(`Checking data file: ${dataPath}`);
      
      try {
        const dataStat = await fs.stat(dataPath);
        if (!dataStat.isFile()) {
          this.logger.warn(`Data path exists but is not a file: ${dataPath}`);
          return null;
        }
      } catch (error) {
        this.logger.warn(`Data file does not exist: ${dataPath} - ${error.message}`);
        return null;
      }

      // 5. Читаем и парсим файл данных
      try {
        this.logger.debug(`Reading data file: ${dataPath}`);
        const fileContent = await fs.readFile(dataPath, 'utf-8');
        this.logger.debug(`File successfully read: ${dataPath} (${fileContent.length} bytes)`);
        
        try {
          const data = JSON.parse(fileContent);
          this.logger.debug(`File successfully parsed as JSON`);
          
          // 6. Ищем запись по телефону
          const record = data[normalizedPhone];
          if (record) {
            this.logger.debug(`Record found for phone: ${normalizedPhone}`);
            return record;
          } else {
            this.logger.debug(`No record found for phone: ${normalizedPhone}`);
            return null;
          }
        } catch (jsonError) {
          this.logger.error(`Error parsing JSON from file ${dataPath}: ${jsonError.message}`);
          return null;
        }
      } catch (readError) {
        this.logger.error(`Error reading file ${dataPath}: ${readError.message}`);
        return null;
      }
    } catch (error) {
      this.logger.error(`Unexpected error in findByPhone: ${error.message}`);
      return null;
    }
  }

  async findByPhoneParallel(phone: string): Promise<any> {
    try {
      const databases = await this.getAllDatabases();
      const batchSize = 3; // Ограничиваем количество одновременных поисков
      const results: (any | null)[] = [];
      
      for (let i = 0; i < databases.length; i += batchSize) {
        const batch = databases.slice(i, i + batchSize);
        const batchPromises = batch.map(async (databaseId) => {
          try {
            // Добавляем искусственную задержку для демонстрации прогресса
            await new Promise(resolve => setTimeout(resolve, 500));
            
            const result = await this.findByPhone(databaseId, phone);
            if (result) {
              return { database: databaseId, ...result };
            }
          } catch (error) {
            this.logger.error(`Error searching in database ${databaseId}: ${error.message}`);
          }
          return null;
        });

        const batchResults = await Promise.all(batchPromises);
        results.push(...batchResults.filter(Boolean));

        // Если нашли результат, прерываем поиск
        if (results.length > 0) {
          break;
        }
      }

      return results[0] || null;
    } catch (error) {
      this.logger.error(`Error in parallel search: ${error.message}`);
      return null;
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
    }) => void
  ): Promise<void> {
    try {
      const databases = await this.getAllDatabases();
      
      // Если нет баз данных, возвращаем сразу
      if (databases.length === 0) {
        progressCallback({
          currentDatabase: '',
          progress: 100,
          searching: false,
          found: false,
          isComplete: true,
          totalDatabases: 0,
          currentDatabaseIndex: 0
        });
        return;
      }
      
      const total = databases.length;

      // Начальное сообщение
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

        // Обновление прогресса поиска
        progressCallback({
          currentDatabase: databaseId,
          progress: currentProgress,
          searching: true,
          found: false,
          isComplete: false,
          totalDatabases: total,
          currentDatabaseIndex: i
        });

        // Добавляем задержку для наглядности
        await new Promise(resolve => setTimeout(resolve, 500));

        try {
          const result = await this.findByPhone(databaseId, phone);
          if (result) {
            // Нашли результат
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
          this.logger.error(`Error searching in database ${databaseId}: ${error.message}`);
        }
      }

      // Ничего не найдено - финальное сообщение
      progressCallback({
        currentDatabase: databases[databases.length - 1],
        progress: 100,
        searching: false,
        found: false,
        result: null,
        isComplete: true,
        totalDatabases: total,
        currentDatabaseIndex: total
      });

    } catch (error) {
      this.logger.error(`Error in search with progress: ${error.message}`);
      progressCallback({
        currentDatabase: '',
        progress: 100,
        searching: false,
        found: false,
        result: null,
        isComplete: true,
        totalDatabases: 0,
        currentDatabaseIndex: 0
      });
    }
  }

  async getAllDatabases(): Promise<string[]> {
    try {
      // Проверяем существование базовой директории
      if (!existsSync(this.baseDir)) {
        await fs.mkdir(this.baseDir, { recursive: true });
        return [];
      }
      
      const directories = await fs.readdir(this.baseDir);
      return directories.filter(dir => {
        try {
          const fullPath = path.join(this.baseDir, dir);
          return existsSync(fullPath) && statSync(fullPath).isDirectory();
        } catch {
          return false;
        }
      });
    } catch (error) {
      this.logger.error(`Error getting databases: ${error.message}`);
      return [];
    }
  }

  async getDatabaseStats(): Promise<DatabaseStatsResponse> {
    try {
      const databases = await this.getAllDatabases();
      const stats: DatabaseStatsResponse = {
        totalDatabases: databases.length,
        totalRecords: 0,
        databases: []
      };

      for (const dbId of databases) {
        try {
          const dbStats = await this.getSingleDatabaseStats(dbId);
          stats.totalRecords += dbStats.totalRecords;
          stats.databases.push({
            id: dbId,
            records: dbStats.totalRecords,
            partitions: dbStats.partitions
          });
        } catch (error) {
          this.logger.error(`Error getting stats for database ${dbId}: ${error.message}`);
          // Добавляем запись с нулевыми значениями, чтобы не пропустить базу
          stats.databases.push({
            id: dbId,
            records: 0,
            partitions: 0
          });
        }
      }

      return stats;
    } catch (error) {
      this.logger.error(`Error getting database stats: ${error.message}`);
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
      const dbPath = path.join(this.baseDir, databaseId);
      
      // Проверяем существование директории базы данных
      if (!existsSync(dbPath)) {
        throw new Error(`Database directory does not exist: ${dbPath}`);
      }
      
      const metadataPath = path.join(dbPath, 'metadata.json');
      
      // Проверяем существование файла метаданных
      if (!existsSync(metadataPath)) {
        throw new Error(`Metadata file does not exist: ${metadataPath}`);
      }
      
      const metadata = JSON.parse(await fs.readFile(metadataPath, 'utf-8'));
      const prefixDirs = await fs.readdir(dbPath);
      const prefixes = prefixDirs.filter(dir => {
        const fullPath = path.join(dbPath, dir);
        return dir !== 'metadata.json' && 
               existsSync(fullPath) && 
               statSync(fullPath).isDirectory();
      });

      return {
        databaseId,
        totalRecords: metadata.totalRecords || 0,
        partitions: metadata.partitionsCount || 0,
        prefixes,
        createdAt: new Date(metadata.createdAt)
      };
    } catch (error) {
      this.logger.error(`Error getting stats for database ${databaseId}: ${error.message}`);
      throw error;
    }
  }
}