import { Injectable, Logger } from '@nestjs/common';
import { S3Service } from './s3.service';
import { SplitFileResult } from '../interfaces/split-file.interface';
import * as csv from 'csv-parser';

@Injectable()
export class FileProcessingService {
    private readonly logger = new Logger(FileProcessingService.name);

    constructor(private readonly s3Service: S3Service) {}

    async splitFile(sourceKey: string): Promise<SplitFileResult> {
        this.logger.log(`Начинаем разбивку файла: ${sourceKey}`);
        const prefixBuffers = new Map<string, string[]>();
        let totalProcessed = 0;
        let headers: string[] = [];

        try {
            const stream = await this.s3Service.getFileStream(sourceKey);

            return new Promise((resolve, reject) => {
                stream
                    .pipe(csv({ separator: '\t' }))
                    .on('headers', (headerRow: string[]) => {
                        headers = headerRow;
                    })
                    .on('data', (row) => {
                        const phone = row.cmainphonenum;
                        if (!phone) {
                            this.logger.warn('Пропущена строка без номера телефона');
                            return;
                        }

                        const prefix = phone.replace(/[^0-9]/g, '').substring(0, 2);
                        if (!prefix) {
                            this.logger.warn(`Невалидный префикс для телефона: ${phone}`);
                            return;
                        }

                        if (!prefixBuffers.has(prefix)) {
                            this.logger.log(`Обнаружен новый префикс: ${prefix}`);
                            prefixBuffers.set(prefix, [headers.join('\t')]); // Добавляем заголовки для нового файла
                        }

                        // Преобразуем строку обратно в CSV формат
                        const rowStr = headers.map(header => row[header]).join('\t');
                        prefixBuffers.get(prefix).push(rowStr);
                        totalProcessed++;

                        if (prefixBuffers.get(prefix).length >= 10000) {
                            this.flushBufferToS3(prefix, prefixBuffers.get(prefix))
                                .catch(error => this.logger.error(`Ошибка при записи буфера: ${error.message}`));
                            prefixBuffers.set(prefix, [headers.join('\t')]); // Сбрасываем буфер, оставляя заголовки
                        }

                        if (totalProcessed % 100000 === 0) {
                            this.logger.log(`Обработано ${totalProcessed} записей`);
                            this.logPrefixStats(prefixBuffers);
                        }
                    })
                    .on('end', async () => {
                        try {
                            // Записываем оставшиеся данные
                            for (const [prefix, buffer] of prefixBuffers.entries()) {
                                if (buffer.length > 1) { // > 1 потому что первая строка - заголовки
                                    await this.flushBufferToS3(prefix, buffer);
                                }
                            }

                            this.logger.log('Процесс разбивки файла успешно завершён');
                            resolve({
                                message: 'Разбивка файла успешно завершена',
                                totalProcessed,
                                filesCreated: prefixBuffers.size
                            });
                        } catch (error) {
                            reject(error);
                        }
                    })
                    .on('error', (error) => {
                        this.logger.error(`Ошибка при чтении файла: ${error.message}`);
                        reject(error);
                    });
            });
        } catch (error) {
            this.logger.error(`Ошибка при обработке файла: ${error.message}`);
            throw error;
        }
    }

    private async flushBufferToS3(prefix: string, buffer: string[]): Promise<void> {
        const content = buffer.join('\n') + '\n';
        const s3Path = `split/${prefix}.csv`;
        
        await this.s3Service.appendToFile(s3Path, content);
        this.logger.log(`Записана порция данных в файл ${s3Path}`);
    }

    private logPrefixStats(prefixBuffers: Map<string, string[]>): void {
        this.logger.log('Статистика по префиксам:');
        for (const [prefix, buffer] of prefixBuffers.entries()) {
            this.logger.log(`Префикс ${prefix}: текущий буфер ${buffer.length} записей`);
        }
    }
}
