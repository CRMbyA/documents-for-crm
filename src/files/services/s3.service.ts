import { Injectable, NotFoundException } from '@nestjs/common';
import { S3, AWSError } from 'aws-sdk';
import { ConfigService } from '@nestjs/config';
import { Readable } from 'stream';
import * as csv from 'csv-parser';
import * as fs from 'fs';

@Injectable()
export class S3Service {
    private s3: S3;
    private readonly bucket: string;

    constructor(private configService: ConfigService) {
        const region = this.configService.get('AWS_REGION');
        this.bucket = this.configService.get('AWS_S3_BUCKET');

        this.s3 = new S3({
            region,
            accessKeyId: this.configService.get('AWS_ACCESS_KEY_ID'),
            secretAccessKey: this.configService.get('AWS_SECRET_ACCESS_KEY'),
            endpoint: `s3.${region}.amazonaws.com`,
            s3ForcePathStyle: false,
            signatureVersion: 'v4'
        });
    }

    async testConnection() {
        try {
            const result = await this.s3.listBuckets().promise();
            console.log('S3 connection successful');
            return true;
        } catch (error) {
            console.error('S3 connection test failed:', error);
            throw error;
        }
    }

    async listFiles(): Promise<Array<{ key: string, size: number, lastModified: Date }>> {
        try {
            const data = await this.s3.listObjectsV2({
                Bucket: this.bucket
            }).promise();

            return data.Contents.map(file => ({
                key: file.Key,
                size: file.Size,
                lastModified: file.LastModified
            }));
        } catch (error) {
            console.error('List files error:', error);
            throw new Error(`Failed to list files: ${error.message}`);
        }
    }

    async getFileStructure(fileName: string): Promise<{
        columns: string[],
        firstRow: Record<string, string>,
        approximateRows: number,
        sizeInBytes: number
    }> {
        const params = {
            Bucket: this.bucket,
            Key: fileName
        };

        try {
            // Получаем метаданные файла через HEAD запрос
            const headObject = await this.s3.headObject(params).promise();
            const sizeInBytes = headObject.ContentLength;

            // Читаем только первые 100KB файла для анализа структуры
            const rangeParams = {
                ...params,
                Range: 'bytes=0-102400' // Первые 100KB
            };

            const s3Stream = (await this.s3.getObject(rangeParams).promise()).Body as Readable;
            const columns: string[] = [];
            let firstRow: Record<string, string> = {};
            let sampleRows = 0;
            const BYTES_PER_LINE_ESTIMATE = 100; // Примерное количество байт на строку

            return new Promise((resolve, reject) => {
                s3Stream
                    .pipe(csv())
                    .on('headers', (headers: string[]) => {
                        columns.push(...headers);
                    })
                    .on('data', (row: Record<string, string>) => {
                        if (sampleRows === 0) {
                            firstRow = row;
                        }
                        sampleRows++;
                    })
                    .on('end', () => {
                        // Примерное количество строк на основе размера файла
                        const approximateRows = Math.floor(sizeInBytes / BYTES_PER_LINE_ESTIMATE);
                        
                        resolve({
                            columns,
                            firstRow,
                            approximateRows,
                            sizeInBytes
                        });
                    })
                    .on('error', (error) => {
                        reject(new Error(`Failed to read file: ${error.message}`));
                    });
            });
        } catch (error) {
            if (error.name === 'NotFound') {
                throw new NotFoundException(`File ${fileName} not found`);
            }
            throw new Error(`Failed to get file structure: ${error.message}`);
        }
    }

    async getFileStream(fileKey: string): Promise<Readable> {
        try {
            const command = {
                Bucket: this.bucket,
                Key: fileKey
            };

            const response = await this.s3.getObject(command).promise();
            return response.Body as Readable;
        } catch (error) {
            if (error.name === 'NoSuchKey') {
                throw new NotFoundException(`File ${fileKey} not found`);
            }
            throw error;
        }
    }

    async getFileStreamByChunks(fileKey: string, startByte: number, endByte: number): Promise<Readable> {
        try {
            const command = {
                Bucket: this.bucket,
                Key: fileKey,
                Range: `bytes=${startByte}-${endByte}`
            };

            const response = await this.s3.getObject(command).promise();
            return response.Body as Readable;
        } catch (error) {
            if (error.name === 'NoSuchKey') {
                throw new NotFoundException(`File ${fileKey} not found`);
            }
            throw error;
        }
    }

    async getFileSize(fileKey: string): Promise<number> {
        try {
            const response = await this.s3.headObject({
                Bucket: this.bucket,
                Key: fileKey
            }).promise();

            return response.ContentLength || 0;
        } catch (error) {
            throw new Error(`Failed to get file size: ${error.message}`);
        }
    }

    async fileExists(fileKey: string): Promise<boolean> {
        try {
            await this.s3.headObject({
                Bucket: this.bucket,
                Key: fileKey
            }).promise();
            return true;
        } catch (error) {
            if (error.name === 'NotFound') {
                return false;
            }
            throw error;
        }
    }

    async uploadFile(fileKey: string, filePath: string): Promise<void> {
        try {
            const fileStream = fs.createReadStream(filePath);
            await this.s3.upload({
                Bucket: this.bucket,
                Key: fileKey,
                Body: fileStream
            }).promise();
        } catch (error) {
            throw new Error(`Failed to upload file: ${error.message}`);
        }
    }

    async appendToFile(fileKey: string, content: string): Promise<void> {
        try {
            await this.s3.putObject({
                Bucket: this.bucket,
                Key: fileKey,
                Body: content,
                ContentType: 'text/csv'
            }).promise();
        } catch (error) {
            throw new Error(`Failed to append to file: ${error.message}`);
        }
    }

    private async streamToString(stream: Readable): Promise<string> {
        return new Promise((resolve, reject) => {
            const chunks: Buffer[] = [];
            stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
            stream.on('error', (err) => reject(err));
            stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
        });
    }
}
