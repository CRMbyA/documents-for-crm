import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { FilesController } from './controllers/files.controller';
import { S3Service } from './services/s3.service';
import { FileProcessingService } from './services/file-processing.service';

@Module({
    imports: [ConfigModule],
    controllers: [FilesController],
    providers: [S3Service, FileProcessingService],
    exports: [S3Service, FileProcessingService]
})
export class FilesModule {}
