import {
    Controller,
    Post,
    Get,
    Body,
    Param,
    UploadedFile,
    UseInterceptors,
    BadRequestException,
    ParseFilePipe,
    MaxFileSizeValidator,
    Delete,
    HttpCode,
    HttpStatus,
    Query
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { 
    ApiTags, 
    ApiOperation, 
    ApiResponse, 
    ApiConsumes, 
    ApiBody,
    ApiBadRequestResponse,
    ApiNotFoundResponse
} from '@nestjs/swagger';
import { memoryStorage } from 'multer';  // Changed from diskStorage to memoryStorage
import { DatabaseService } from '../services/database.service';
import { ImportService } from '../services/import.service';
import { CreateDatabaseDto } from '../dto/create-database.dto';
import { UploadFileDto } from '../dto/upload-file.dto';
import { DatabaseResponseDto } from '../dto/database.response.dto';
import { ImportResultDto } from '../dto/import-result.dto';
import { DatabaseStatsDto } from '../dto/database-stats.dto';

const MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024; // 10GB

@ApiTags('Database Import')
@Controller('import')
export class ImportController {
    constructor(
        private readonly databaseService: DatabaseService,
        private readonly importService: ImportService,
    ) {}

    @Post('database')
    @ApiOperation({ 
        summary: 'Create new database',
        description: 'Creates a new database for subsequent client data import'
    })
    @ApiResponse({
        status: 201,
        description: 'Database has been successfully created',
        type: DatabaseResponseDto
    })
    @ApiBadRequestResponse({
        description: 'Validation error: invalid database name or database already exists'
    })
    async createDatabase(
        @Body() createDatabaseDto: CreateDatabaseDto
    ): Promise<DatabaseResponseDto> {
        return this.databaseService.create(createDatabaseDto);
    }

    @Get('databases')
    @ApiOperation({ 
        summary: 'Get all databases',
        description: 'Returns a list of all databases with their record counts'
    })
    @ApiResponse({
        status: 200,
        description: 'List of databases retrieved successfully',
        type: [DatabaseResponseDto]
    })
    async getAllDatabases(): Promise<DatabaseResponseDto[]> {
        return this.databaseService.findAll();
    }

    @Get('databases/:id')
    @ApiOperation({ 
        summary: 'Get database details',
        description: 'Returns details of a specific database'
    })
    @ApiResponse({
        status: 200,
        description: 'Database details retrieved successfully',
        type: DatabaseResponseDto
    })
    @ApiNotFoundResponse({
        description: 'Database not found'
    })
    async getDatabase(
        @Param('id') id: string
    ): Promise<DatabaseResponseDto> {
        return this.databaseService.findOne(id);
    }

    @Get('databases/:id/stats')
    @ApiOperation({ 
        summary: 'Get database statistics',
        description: 'Returns detailed statistics for a specific database'
    })
    @ApiResponse({
        status: 200,
        description: 'Database statistics retrieved successfully',
        type: DatabaseStatsDto
    })
    @ApiNotFoundResponse({
        description: 'Database not found'
    })
    async getDatabaseStats(
        @Param('id') id: string
    ): Promise<DatabaseStatsDto> {
        return this.databaseService.getDatabaseStats(id);
    }

    @Post('upload')
    @ApiOperation({ 
        summary: 'Upload client data file',
        description: 'Uploads and processes a file containing client data (up to 10GB). Supports CSV and TXT formats.'
    })
    @ApiConsumes('multipart/form-data')
    @ApiBody({
        schema: {
            type: 'object',
            required: ['file', 'databaseId'],
            properties: {
                file: {
                    type: 'string',
                    format: 'binary',
                    description: 'Client data file (CSV or TXT). Maximum size: 10GB'
                },
                databaseId: {
                    type: 'string',
                    format: 'uuid',
                    description: 'Target database ID for data import'
                }
            }
        }
    })
    @ApiResponse({
        status: 201,
        description: 'File has been successfully processed',
        type: ImportResultDto
    })
    @UseInterceptors(
        FileInterceptor('file', {
            storage: memoryStorage(),
            limits: {
                fileSize: MAX_FILE_SIZE
            },
            fileFilter: (req, file, cb) => {
                // Validate file extension
                if (!file.originalname.match(/\.(txt|csv)$/)) {
                    return cb(new BadRequestException('Only .txt and csv files are allowed'), false);
                }
                
                // Validate MIME type
                const allowedMimeTypes = ['text/plain', 'text/csv', 'application/csv', 'application/vnd.ms-excel'];
                if (!allowedMimeTypes.includes(file.mimetype)) {
                    return cb(new BadRequestException('Invalid file type'), false);
                }
                
                cb(null, true);
            }
        })
    )
    async uploadFile(
        @UploadedFile(
            new ParseFilePipe({
                validators: [
                    new MaxFileSizeValidator({ maxSize: MAX_FILE_SIZE })
                ],
            }),
        )
        file: Express.Multer.File,
        @Body() uploadFileDto: UploadFileDto
    ): Promise<ImportResultDto> {
        return this.importService.importFile(file, uploadFileDto.databaseId);
    }

    @Delete('databases/:id')
    @ApiOperation({ 
        summary: 'Delete database',
        description: 'Deletes database and all its clients'
    })
    @ApiResponse({
        status: 200,
        description: 'Database has been successfully deleted'
    })
    @ApiNotFoundResponse({
        description: 'Database not found'
    })
    @HttpCode(HttpStatus.OK)
    async removeDatabase(@Param('id') id: string): Promise<void> {
        await this.databaseService.remove(id);
    }

    @Get('search-by-phone')
@ApiOperation({ 
    summary: 'Search client by phone',
    description: 'Search for a random client by phone number across all databases'
})
@ApiResponse({
    status: 200,
    description: 'Random client found by phone number',
    schema: {
        type: 'object',
        properties: {
            id: {
                type: 'string',
                format: 'uuid',
                description: 'Client ID'
            },
            full_name: {
                type: 'string',
                description: 'Client full name'
            },
            phone: {
                type: 'string',
                description: 'Client phone number'
            },
            email: {
                type: 'string',
                description: 'Client email'
            },
            database_name: {
                type: 'string',
                description: 'Database name'
            },
            database_id: {
                type: 'string',
                format: 'uuid',
                description: 'Database ID'
            }
        }
    }
})
async searchByPhone(
    @Query('phone') phone: string
): Promise<{
    id: string;
    full_name: string;
    phone: string;
    email: string;
    database_name: string;
    database_id: string;
} | null> {
    const results = await this.databaseService.findClientsByPhone(phone);
    return results[0] || null;
}
}