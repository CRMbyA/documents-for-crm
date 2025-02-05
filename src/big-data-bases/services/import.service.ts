import { Injectable, BadRequestException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { DatabaseClient } from '../entities/database-client.entity';
import { DatabaseService } from './database.service';
import { ImportResultDto } from '../dto/import-result.dto';
import { ContinueImportResultDto } from '../dto/continue-import-result.dto';
import * as iconv from 'iconv-lite';

@Injectable()
export class ImportService {
    private readonly batchSize = 2000;
    
    constructor(
        @InjectRepository(DatabaseClient)
        private clientRepository: Repository<DatabaseClient>,
        private databaseService: DatabaseService,
    ) {}

    private clearAndLogProgress(
        processedCount: number,
        successCount: number,
        errorCount: number,
        currentLineNumber: number,
        totalChunks: number,
        processedChunks: number,
        startTime: number,
        lastLogTime: number,
        force = false
    ): number {
        const currentTime = Date.now();
        const elapsedSeconds = (currentTime - startTime) / 1000;
        const timeSinceLastLog = currentTime - lastLogTime;

        if (force || timeSinceLastLog >= 5000) {
            const progress = (processedChunks / totalChunks) * 100;
            const rowsPerSecond = processedCount / elapsedSeconds;
            const memoryUsage = process.memoryUsage();

            console.log(`
=================== РЎС‚Р°С‚СѓСЃ РёРјРїРѕСЂС‚Р° ===================
РџСЂРѕРіСЂРµСЃСЃ: ${progress.toFixed(2)}%
РћР±СЂР°Р±РѕС‚Р°РЅРѕ СЃС‚СЂРѕРє: ${processedCount.toLocaleString()}
РЈСЃРїРµС€РЅРѕ: ${successCount.toLocaleString()}
РћС€РёР±РѕРє: ${errorCount}
РўРµРєСѓС‰Р°СЏ СЃС‚СЂРѕРєР°: ${currentLineNumber}
РЎРєРѕСЂРѕСЃС‚СЊ: ${Math.round(rowsPerSecond)} СЃС‚СЂРѕРє/СЃРµРє
РџР°РјСЏС‚СЊ: RSS ${Math.round(memoryUsage.rss / 1024 / 1024)}MB | Heap ${Math.round(memoryUsage.heapUsed / 1024 / 1024)}MB
====================================================
            `);

            return currentTime;
        }
        return lastLogTime;
    }

    private clearMemory() {
        if (global.gc) {
            global.gc();
        }
        const used = process.memoryUsage();
        console.log(`РћС‡РёСЃС‚РєР° РїР°РјСЏС‚Рё:
            RSS: ${Math.round(used.rss / 1024 / 1024)} MB
            Heap: ${Math.round(used.heapUsed / 1024 / 1024)} MB`);
    }

    async continueImportFromLine(
        file: Express.Multer.File, 
        databaseId: string, 
        startFromLine: number,
        timeout: number = 600000 
    ): Promise<ContinueImportResultDto> {
        try {
            if (!file || !file.buffer) {
                throw new BadRequestException('Invalid file uploaded');
            }
    
            const database = await this.databaseService.findOne(databaseId);
            if (!database) {
                throw new BadRequestException('Database not found');
            }
    
            let isTimedOut = false;
            const timeoutId = setTimeout(() => {
                isTimedOut = true;
                console.log('Операция прервана по таймауту');
            }, timeout);
    
            const chunkSize = 5 * 1024 * 1024; // увеличим до 5MB
            const totalSize = file.buffer.length;
            const totalChunks = Math.ceil(totalSize / chunkSize);
            const batchSize = 5000; // увеличенный размер батча
    
            console.log(`
    Начало импорта:
    Размер файла: ${(totalSize / (1024 * 1024)).toFixed(2)} MB
    Всего чанков: ${totalChunks}
    Начальная строка: ${startFromLine}
            `);
    
            let processedChunks = 0;
            let processedCount = 0;
            let successCount = 0;
            let errorCount = 0;
            let remainder = Buffer.from([]);
            let currentLineNumber = 0;
            let isFirstChunk = true;
            let currentBatch: DatabaseClient[] = [];
    
            const startTime = Date.now();
            let lastLogTime = startTime;
    
            for (let offset = 0; offset < file.buffer.length; offset += chunkSize) {
                if (isTimedOut) break;
    
                const chunk = file.buffer.slice(offset, offset + chunkSize);
                const bufferWithRemainder = Buffer.concat([remainder, chunk]);
                const text = iconv.decode(bufferWithRemainder, 'win1251');
                const lines = text.split('\n');
    
                remainder = Buffer.from([]);
    
                if (isFirstChunk) {
                    lines.shift();
                    isFirstChunk = false;
                }
    
                for (const line of lines) {
                    currentLineNumber++;
                    
                    if (currentLineNumber < startFromLine) continue;
                    if (!line.trim()) continue;
    
                    const [_, fullName, birthDate, phone] = line.split(',').map(field => field?.trim());
    
                    let parsedBirthDate: Date | null = null;
                    if (birthDate) {
                        const [day, month, year] = birthDate.split('.').map(num => parseInt(num.trim()));
                        if (!isNaN(day) && !isNaN(month) && !isNaN(year)) {
                            parsedBirthDate = new Date(year, month - 1, day);
                        }
                    }
    
                    const client = this.clientRepository.create({
                        full_name: fullName || '',
                        birth_date: parsedBirthDate ? parsedBirthDate.toISOString() : null,
                        phone: phone || '',
                        database: { id: databaseId }
                    });
    
                    currentBatch.push(client);
                    processedCount++;
    
                    if (currentBatch.length >= batchSize) {
                        try {
                            await this.clientRepository
                                .createQueryBuilder()
                                .insert()
                                .into(DatabaseClient)
                                .values(currentBatch)
                                .orIgnore()
                                .execute();
    
                            successCount += currentBatch.length;
                        } catch (error) {
                            console.error('Batch insert failed:', error);
                            errorCount += currentBatch.length;
                        }
    
                        currentBatch = [];
    
                        const currentTime = Date.now();
                        if (currentTime - lastLogTime >= 10000) {
                            console.log(`Progress: ${processedCount} processed, ${successCount} success, ${errorCount} errors`);
                            lastLogTime = currentTime;
                        }
                    }
                }
    
                if (currentBatch.length > 0) {
                    try {
                        await this.clientRepository
                            .createQueryBuilder()
                            .insert()
                            .into(DatabaseClient)
                            .values(currentBatch)
                            .orIgnore()
                            .execute();
    
                        successCount += currentBatch.length;
                    } catch (error) {
                        console.error('Final batch insert failed:', error);
                        errorCount += currentBatch.length;
                    }
                }
    
                processedChunks++;
                this.clearMemory(); // очистка памяти только после обработки чанка
            }
    
            clearTimeout(timeoutId);
    
            return {
                totalProcessed: processedCount,
                successCount,
                errorCount,
                message: isTimedOut ? 'Импорт прерван по таймауту' : 'Импорт успешно завершен',
                lastProcessedLine: currentLineNumber,
                startFromLine: startFromLine,
                status: isTimedOut ? 'timeout' : 'completed'
            };
    
        } catch (error) {
            console.error('Import failed:', error);
            throw new BadRequestException(`Import failed: ${error.message}`);
        }
    }
}