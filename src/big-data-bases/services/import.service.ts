// ANALYSIS OF THE ISSUE:
// 1. The current implementation tries to process 4.4M records in batches of 50k
// 2. Memory management issues may occur due to:
//    - Keeping all lines in memory after splitting
//    - No stream processing
//    - Large batch size may cause memory spikes

// IMPROVED IMPLEMENTATION:
import { Injectable, BadRequestException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { DatabaseClient } from '../entities/database-client.entity';
import { DatabaseService } from './database.service';
import { ImportResultDto } from '../dto/import-result.dto';
import { promises as fs } from 'fs';
import * as iconv from 'iconv-lite';
import { createReadStream } from 'fs';
import * as readline from 'readline';

interface ImportStats {
    processed: number;
    success: number;
    errors: number;
    elapsed: number;
}

@Injectable()
export class ImportService {
    private readonly batchSize = 10000;
    
    constructor(
        @InjectRepository(DatabaseClient)
        private clientRepository: Repository<DatabaseClient>,
        private databaseService: DatabaseService,
    ) {}

    private async processLineStream(
        fileStream: NodeJS.ReadableStream,
        databaseId: string,
        onProgress: (stats: ImportStats) => void
    ): Promise<ImportResultDto> {
        const startTime = Date.now();
        let currentBatch: DatabaseClient[] = [];
        let processedCount = 0;
        let successCount = 0;
        let errorCount = 0;
        let isFirstLine = true;

        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity
        });

        for await (const line of rl) {
            // Skip header
            if (isFirstLine) {
                isFirstLine = false;
                continue;
            }

            if (line.trim()) {
                const [name, phone, address] = line.split('\t').map(field => field?.trim());
                
                const client = this.clientRepository.create({
                    full_name: name || '',
                    phone: phone || '',
                    address: address || '',
                    database: { id: databaseId }
                });

                currentBatch.push(client);
                processedCount++;

                if (currentBatch.length >= this.batchSize) {
                    try {
                        await this.clientRepository
                            .createQueryBuilder()
                            .insert()
                            .into(DatabaseClient)
                            .values(currentBatch)
                            .execute();
                        
                        successCount += currentBatch.length;
                    } catch (error) {
                        errorCount += currentBatch.length;
                        console.error(`Batch insert failed:`, error);
                    }

                    // Report progress
                    onProgress({
                        processed: processedCount,
                        success: successCount,
                        errors: errorCount,
                        elapsed: Date.now() - startTime
                    });

                    // Clear batch
                    currentBatch = [];
                }
            }
        }

        // Process remaining records
        if (currentBatch.length > 0) {
            try {
                await this.clientRepository
                    .createQueryBuilder()
                    .insert()
                    .into(DatabaseClient)
                    .values(currentBatch)
                    .execute();
                
                successCount += currentBatch.length;
            } catch (error) {
                errorCount += currentBatch.length;
                console.error(`Final batch insert failed:`, error);
            }
        }

        return {
            totalProcessed: processedCount,
            successCount,
            errorCount,
            message: 'Import completed'
        };
    }

    async importFile(file: Express.Multer.File, databaseId: string): Promise<ImportResultDto> {
        const startTime = Date.now();
        let lastStatsTime = Date.now();
        const statsInterval = 20000; // 20 seconds

        try {
            const database = await this.databaseService.findOne(databaseId);
            console.log(`Database found: ${database.name}`);

            // Create a read stream instead of loading entire file
            const fileStream = createReadStream(file.path);
            const decodingStream = iconv.decodeStream('win1251');
            
            const result = await this.processLineStream(
                fileStream.pipe(decodingStream),
                databaseId,
                (stats) => {
                    const currentTime = Date.now();
                    if (currentTime - lastStatsTime >= statsInterval) {
                        console.log('Import progress:', stats);
                        lastStatsTime = currentTime;
                    }
                }
            );

            return result;

        } catch (error) {
            console.error('Import failed:', error);
            throw new BadRequestException(`Import failed: ${error.message}`);
        } finally {
            try {
                await fs.unlink(file.path);
                console.log('Temporary file cleaned up');
            } catch (error) {
                console.error('Failed to clean up temporary file:', error);
            }
        }
    }
}