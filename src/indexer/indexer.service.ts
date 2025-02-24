import { Injectable, Logger } from '@nestjs/common';
import { Worker } from 'worker_threads';
import * as fs from 'fs/promises';
import { statSync } from 'fs';
import * as path from 'path';
import * as csv from 'csv-parser';
import { CreateIndexDto } from './dto/create-index.dto';
import { IndexMetadata, IndexedData, Partition, DatabaseStats, DatabaseStatsResponse } from './types/index.types';
import { createReadStream } from 'fs';

@Injectable()
export class IndexerService {
  private readonly logger = new Logger(IndexerService.name);
  private readonly baseDir = './uploads';
  private readonly PARTITION_SIZE = 1000; // Увеличиваем размер партиции
  private readonly CHUNK_SIZE = 5000; // Размер порции данных для обработки

  async createIndex(filePath: string, dto: CreateIndexDto): Promise<IndexMetadata> {
    const databaseDir = path.join(this.baseDir, dto.databaseId);
    await fs.mkdir(databaseDir, { recursive: true });

    const metadata: IndexMetadata = {
      id: Date.now().toString(),
      originalFileName: path.basename(filePath),
      totalRecords: 0,
      partitionsCount: 0,
      partitionSize: this.PARTITION_SIZE,
      createdAt: new Date(),
      phoneColumn: 'cmainphonenum'
    };

    let buffer: { [key: string]: any } = {};
    let processed = 0;

    return new Promise<IndexMetadata>((resolve, reject) => {
      createReadStream(filePath)
        .pipe(csv({ separator: '\t' }))
        .on('data', (row) => {
          const phone = row['cmainphonenum']?.toString().trim();
          if (phone) {
            const firstDigits = phone.substring(0, 3); // Группируем по первым цифрам номера
            if (!buffer[firstDigits]) {
              buffer[firstDigits] = {};
            }
            
            buffer[firstDigits][phone] = {
              icusid: row['icusid'],
              ccusfullname: row['ccusfullname'],
              ccusinn: row['ccusinn'],
              cgender: row['cgender'],
              dbirthdate: row['dbirthdate'],
              phone: phone
            };

            processed++;
            metadata.totalRecords++;

            // Сохраняем когда накопилось достаточно записей
            if (processed >= this.CHUNK_SIZE) {
              this.saveBufferToDisk(databaseDir, buffer);
              buffer = {};
              processed = 0;
              this.logger.log(`Processed ${metadata.totalRecords} records`);
            }
          }
        })
        .on('end', async () => {
          if (Object.keys(buffer).length > 0) {
            await this.saveBufferToDisk(databaseDir, buffer);
          }
          
          metadata.partitionsCount = Math.ceil(metadata.totalRecords / this.PARTITION_SIZE);
          await this.saveMetadata(databaseDir, metadata);
          this.logger.log(`Indexing completed. Total records: ${metadata.totalRecords}`);
          resolve(metadata);
        })
        .on('error', reject);
    });
  }

  private async saveBufferToDisk(databaseDir: string, buffer: { [key: string]: any }): Promise<void> {
    for (const prefix of Object.keys(buffer)) {
      const prefixDir = path.join(databaseDir, prefix);
      await fs.mkdir(prefixDir, { recursive: true });
      await fs.writeFile(
        path.join(prefixDir, 'data.json'),
        JSON.stringify(buffer[prefix])
      );
    }
  }

  private async saveMetadata(databaseDir: string, metadata: IndexMetadata): Promise<void> {
    try {
      const metadataPath = path.join(databaseDir, 'metadata.json');
      await fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2));
      this.logger.log(`Metadata saved to ${metadataPath}`);
    } catch (error) {
      this.logger.error(`Error saving metadata: ${error.message}`);
      throw error;
    }
  }

  async findByPhone(databaseId: string, phone: string): Promise<any | null> {
    try {
      const prefix = phone.substring(0, 3);
      const dataPath = path.join(this.baseDir, databaseId, prefix, 'data.json');
      const data = JSON.parse(await fs.readFile(dataPath, 'utf-8'));
      return data[phone] || null;
    } catch {
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
        results.push(...batchResults);

        // Если нашли результат, прерываем поиск
        if (results.some(r => r !== null)) {
          break;
        }
      }

      return results.find(result => result !== null) || null;
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
      const directories = await fs.readdir(this.baseDir);
      return directories.filter(dir => {
        try {
          return statSync(path.join(this.baseDir, dir)).isDirectory();
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
        const dbStats = await this.getSingleDatabaseStats(dbId);
        stats.totalRecords += dbStats.totalRecords;
        stats.databases.push({
          id: dbId,
          records: dbStats.totalRecords,
          partitions: dbStats.partitions
        });
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
      const metadataPath = path.join(this.baseDir, databaseId, 'metadata.json');
      const metadata = JSON.parse(await fs.readFile(metadataPath, 'utf-8'));
      const prefixDirs = await fs.readdir(path.join(this.baseDir, databaseId));
      const prefixes = prefixDirs.filter(dir => 
        dir !== 'metadata.json' && 
        statSync(path.join(this.baseDir, databaseId, dir)).isDirectory()
      );

      return {
        databaseId,
        totalRecords: metadata.totalRecords,
        partitions: metadata.partitionsCount,
        prefixes,
        createdAt: new Date(metadata.createdAt)
      };
    } catch (error) {
      this.logger.error(`Error getting stats for database ${databaseId}: ${error.message}`);
      throw error;
    }
  }
}
