import { Controller, Post, Get, Param, Sse, Logger, HttpException, HttpStatus } from '@nestjs/common';
import { ApiTags, ApiOperation } from '@nestjs/swagger';
import { IndexerService } from './indexer.service';
import * as path from 'path';
import * as fs from 'fs';
import { Observable, Subject } from 'rxjs';
import { DatabaseStatsResponse } from './types/index.types';

// Определяем интерфейсы для типизации
interface PrefixInfo {
  prefix: string;
  isDirectory: boolean;
  dataFileExists: boolean;
}

interface DatabaseInfo {
  name: string;
  isDirectory: boolean;
  prefixes: PrefixInfo[];
  metadataExists: boolean;
}

interface DirectoryTestResult {
  uploadDirExists: boolean;
  directories: DatabaseInfo[];
  errors: string[];
}

@ApiTags('Индексация')
@Controller('indexer')
export class IndexerController {
  private readonly logger = new Logger(IndexerController.name);

  constructor(private readonly indexerService: IndexerService) {
    this.logger.log('IndexerController initialized');
    
    // Создаем директорию uploads, если она не существует
    const uploadDir = path.join(process.cwd(), 'uploads');
    if (!fs.existsSync(uploadDir)) {
      this.logger.log(`Creating uploads directory: ${uploadDir}`);
      fs.mkdirSync(uploadDir, { recursive: true });
    }
  }

  @Post(':filename')
  @ApiOperation({ summary: 'Создать индекс для файла' })
  async createIndex(@Param('filename') filename: string) {
    this.logger.log(`Request to create index for file: ${filename}`);
    
    try {
      // Проверка различных вариантов пути к файлу
      let filePath = '';
      let fileExists = false;
      
      // 1. Проверяем, если это абсолютный путь
      if (path.isAbsolute(filename)) {
        this.logger.log(`Checking absolute path: ${filename}`);
        if (fs.existsSync(filename)) {
          filePath = filename;
          fileExists = true;
          this.logger.log(`File found at absolute path: ${filePath}`);
        }
      }
      
      // 2. Проверяем в директории uploads
      if (!fileExists) {
        const relativePath = path.join(process.cwd(), 'uploads', path.basename(filename));
        this.logger.log(`Checking in uploads dir: ${relativePath}`);
        if (fs.existsSync(relativePath)) {
          filePath = relativePath;
          fileExists = true;
          this.logger.log(`File found in uploads directory: ${filePath}`);
        }
      }
      
      // 3. Проверяем в корне проекта
      if (!fileExists) {
        const projectRootPath = path.join(process.cwd(), path.basename(filename));
        this.logger.log(`Checking in project root: ${projectRootPath}`);
        if (fs.existsSync(projectRootPath)) {
          filePath = projectRootPath;
          fileExists = true;
          this.logger.log(`File found in project root: ${filePath}`);
        }
      }
      
      // Если файл не найден
      if (!fileExists) {
        this.logger.error(`File not found: ${filename}`);
        throw new HttpException(`File not found: ${filename}`, HttpStatus.NOT_FOUND);
      }
      
      // Проверяем, что это файл, а не директория
      const stats = fs.statSync(filePath);
      if (!stats.isFile()) {
        this.logger.error(`Path exists but is not a file: ${filePath}`);
        throw new HttpException(`Path is not a file: ${filePath}`, HttpStatus.BAD_REQUEST);
      }
      
      // Выводим информацию о размере файла
      this.logger.log(`File size: ${stats.size} bytes`);
      
      // Определяем ID базы данных из имени файла
      const databaseId = path.basename(filePath, path.extname(filePath));
      this.logger.log(`Using database ID: ${databaseId}`);

      // Запускаем создание индекса
      this.logger.log(`Starting index creation for ${filePath} with database ID ${databaseId}`);
      const result = await this.indexerService.createIndex(filePath, {
        databaseId,
        phoneColumn: 'cmainphonenum', // Для txt файлов игнорируется
        partitionSize: 100
      });
      
      this.logger.log(`Index created successfully. Total records: ${result.totalRecords}`);
      return result;
    } catch (error) {
      this.logger.error(`Error creating index for ${filename}: ${error.message}`, error.stack);
      throw new HttpException(
        `Failed to create index: ${error.message}`, 
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }

  @Sse('search-stream/:phone')
  @ApiOperation({ summary: 'Потоковый поиск по номеру телефона с прогрессом' })
  searchPhoneStream(@Param('phone') phone: string): Observable<any> {
    this.logger.log(`Starting streaming search for phone: ${phone}`);
    const subject = new Subject();

    this.indexerService.findByPhoneWithProgress(phone, (progress) => {
      // Логируем каждое обновление прогресса
      if (progress.searching) {
        this.logger.debug(`Search progress: ${progress.progress}% in database ${progress.currentDatabase}`);
      } else if (progress.found) {
        this.logger.log(`Found record for phone ${phone} in database ${progress.currentDatabase}`);
      } else if (progress.isComplete) {
        this.logger.log(`Search completed for phone ${phone}. Record ${progress.found ? 'found' : 'not found'}`);
      }
      
      // Отправляем каждое обновление как есть
      subject.next({ data: progress });

      // Закрываем поток только когда поиск действительно завершен
      if (progress.isComplete) {
        this.logger.log(`Closing stream for phone ${phone}`);
        setTimeout(() => subject.complete(), 100);
      }
    });

    return subject.asObservable();
  }

  @Get('search/:phone')
  @ApiOperation({ summary: 'Быстрый поиск по номеру телефона' })
  async findByPhone(@Param('phone') phone: string) {
    this.logger.log(`Performing quick search for phone: ${phone}`);
    
    try {
      const result = await this.indexerService.findByPhoneParallel(phone);
      
      if (result) {
        this.logger.log(`Found record for phone ${phone}`);
      } else {
        this.logger.log(`No record found for phone ${phone}`);
      }
      
      return result;
    } catch (error) {
      this.logger.error(`Error searching for phone ${phone}: ${error.message}`, error.stack);
      throw new HttpException(
        `Failed to search: ${error.message}`, 
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }

  @Get('stats')
  @ApiOperation({ summary: 'Получить статистику по всем базам данных' })
  async getDatabaseStats(): Promise<DatabaseStatsResponse> {
    this.logger.log('Getting statistics for all databases');
    
    try {
      const stats = await this.indexerService.getDatabaseStats();
      this.logger.log(`Retrieved stats for ${stats.totalDatabases} databases with ${stats.totalRecords} total records`);
      return stats;
    } catch (error) {
      this.logger.error(`Error getting database stats: ${error.message}`, error.stack);
      throw new HttpException(
        `Failed to get stats: ${error.message}`, 
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }

  @Get('stats/:databaseId')
  @ApiOperation({ summary: 'Получить статистику конкретной базы данных' })
  async getSingleDatabaseStats(@Param('databaseId') databaseId: string) {
    this.logger.log(`Getting statistics for database: ${databaseId}`);
    
    try {
      const stats = await this.indexerService.getSingleDatabaseStats(databaseId);
      this.logger.log(`Retrieved stats for database ${databaseId}: ${stats.totalRecords} records in ${stats.partitions} partitions`);
      return stats;
    } catch (error) {
      this.logger.error(`Error getting stats for database ${databaseId}: ${error.message}`, error.stack);
      throw new HttpException(
        `Failed to get stats for database ${databaseId}: ${error.message}`, 
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }

  @Get('test-directories')
  @ApiOperation({ summary: 'Проверить структуру директорий и файлов' })
  async testDirectories() {
    this.logger.log('Testing directory structure');
    
    try {
      // Проверяем структуру директорий и файлов
      const uploadPath = path.join(process.cwd(), 'uploads');
      this.logger.log(`Base upload path: ${uploadPath}`);
      
      // Определяем результат с правильными типами
      const result: DirectoryTestResult = {
        uploadDirExists: false,
        directories: [],
        errors: []
      };
      
      // Проверяем существование корневой директории
      if (fs.existsSync(uploadPath)) {
        result.uploadDirExists = true;
        this.logger.log(`Upload directory exists: ${uploadPath}`);
        
        // Получаем список баз данных
        const entries = fs.readdirSync(uploadPath);
        this.logger.log(`Found ${entries.length} entries in upload directory`);
        
        const databases = entries.filter(dir => {
          const fullPath = path.join(uploadPath, dir);
          return fs.existsSync(fullPath) && fs.statSync(fullPath).isDirectory();
        });
        
        this.logger.log(`Found ${databases.length} database directories`);
        
        // Проверяем каждую базу данных
        for (const db of databases) {
          try {
            const dbPath = path.join(uploadPath, db);
            this.logger.log(`Checking database: ${db} at ${dbPath}`);
            
            const dbInfo: DatabaseInfo = {
              name: db,
              isDirectory: fs.statSync(dbPath).isDirectory(),
              prefixes: [],
              metadataExists: false
            };
            
            // Проверяем наличие metadata.json
            const metadataPath = path.join(dbPath, 'metadata.json');
            if (fs.existsSync(metadataPath)) {
              dbInfo.metadataExists = fs.statSync(metadataPath).isFile();
              this.logger.log(`Metadata file exists: ${metadataPath} (isFile: ${dbInfo.metadataExists})`);
            } else {
              this.logger.warn(`Metadata file does not exist: ${metadataPath}`);
            }
            
            // Проверяем префиксы (директории с первыми цифрами номеров)
            const dbEntries = fs.readdirSync(dbPath);
            this.logger.log(`Found ${dbEntries.length} entries in database ${db}`);
            
            const prefixes = dbEntries.filter(item => {
              const itemPath = path.join(dbPath, item);
              return item !== 'metadata.json' && 
                fs.existsSync(itemPath) && 
                fs.statSync(itemPath).isDirectory();
            });
            
            this.logger.log(`Found ${prefixes.length} prefix directories in database ${db}`);
            
            for (const prefix of prefixes) {
              const prefixPath = path.join(dbPath, prefix);
              this.logger.log(`Checking prefix: ${prefix} at ${prefixPath}`);
              
              const dataPath = path.join(prefixPath, 'data.json');
              const dataFileExists = fs.existsSync(dataPath);
              const isDataFile = dataFileExists ? fs.statSync(dataPath).isFile() : false;
              
              this.logger.log(`Data file ${dataPath}: exists=${dataFileExists}, isFile=${isDataFile}`);
              
              dbInfo.prefixes.push({
                prefix,
                isDirectory: fs.statSync(prefixPath).isDirectory(),
                dataFileExists: isDataFile
              });
            }
            
            result.directories.push(dbInfo);
          } catch (dbError) {
            const errorMsg = `Error checking database ${db}: ${dbError.message}`;
            this.logger.error(errorMsg, dbError.stack);
            result.errors.push(errorMsg);
          }
        }
      } else {
        this.logger.warn(`Upload directory does not exist: ${uploadPath}`);
      }
      
      return {
        success: true,
        data: result
      };
    } catch (error) {
      this.logger.error(`Error in test-directories: ${error.message}`, error.stack);
      throw new HttpException(
        `Error testing directories: ${error.message}`, 
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }
}