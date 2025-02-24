import { Controller, Post, Body, Get, Param, Query, HttpStatus } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiBody, ApiProperty } from '@nestjs/swagger';
import { FileProcessingService } from '../services/file-processing.service';
import { S3Service } from '../services/s3.service';
import { SplitFileResult } from '../interfaces/split-file.interface';

export class SplitFileDto {
    @ApiProperty({
        description: 'The key of the source file in S3 to split',
        example: 'data/source-file.csv'
    })
    sourceKey: string;
}

export class SplitFileResponseDto {
    @ApiProperty({ example: 'File split complete' })
    message: string;

    @ApiProperty({ example: 1000000 })
    totalProcessed: number;

    @ApiProperty({ example: 10 })
    filesCreated: number;
}

export class FileMetadataDto {
    @ApiProperty()
    key: string;

    @ApiProperty()
    size: number;

    @ApiProperty()
    lastModified: Date;
}

@ApiTags('Files')
@Controller('files')
export class FilesController {
    constructor(
        private readonly s3Service: S3Service,
        private readonly fileProcessingService: FileProcessingService
    ) {}

    @Get()
    @ApiOperation({ summary: 'List all files in S3 bucket' })
    @ApiResponse({
        status: HttpStatus.OK,
        description: 'Returns list of files with their metadata',
        type: [FileMetadataDto]
    })
    async listFiles(): Promise<FileMetadataDto[]> {
        return this.s3Service.listFiles();
    }

    @Get(':fileKey/structure')
    @ApiOperation({ summary: 'Get CSV file structure' })
    @ApiResponse({
        status: HttpStatus.OK,
        description: 'Returns file structure information including columns and sample data',
        schema: {
            properties: {
                columns: { type: 'array', items: { type: 'string' } },
                firstRow: { type: 'object' },
                approximateRows: { type: 'number' },
                sizeInBytes: { type: 'number' }
            }
        }
    })
    @ApiResponse({ status: HttpStatus.NOT_FOUND, description: 'File not found' })
    async getFileStructure(@Param('fileKey') fileKey: string) {
        return this.s3Service.getFileStructure(fileKey);
    }

    @Post('split')
    @ApiOperation({
        summary: 'Split CSV file by phone number prefix',
        description: 'Splits a CSV file into multiple files based on phone number prefixes'
    })
    @ApiBody({ type: SplitFileDto })
    @ApiResponse({
        status: HttpStatus.OK,
        description: 'File successfully split',
        type: SplitFileResponseDto
    })
    @ApiResponse({ 
        status: HttpStatus.BAD_REQUEST,
        description: 'Invalid file format or processing error'
    })
    async splitFile(@Body() body: SplitFileDto): Promise<SplitFileResponseDto> {
        const result: SplitFileResult = await this.fileProcessingService.splitFile(body.sourceKey);
        return {
            message: result.message,
            totalProcessed: result.totalProcessed,
            filesCreated: result.filesCreated
        };
    }
}
