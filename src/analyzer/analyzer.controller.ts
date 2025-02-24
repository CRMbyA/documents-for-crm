import { Controller, Post, UploadedFile, UseInterceptors } from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { ApiTags, ApiOperation, ApiConsumes, ApiBody } from '@nestjs/swagger';
import { AnalyzerService } from './analyzer.service';
import { AnalysisResult } from './types/analysis.types';
import { diskStorage } from 'multer';
import * as path from 'path';

@ApiTags('Анализ CSV')
@Controller('analyzer')
export class AnalyzerController {
  constructor(private readonly analyzerService: AnalyzerService) {}

  @Post('analyze')
  @ApiOperation({ summary: 'Анализ CSV файла' })
  @ApiConsumes('multipart/form-data')
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        file: {
          type: 'string',
          format: 'binary',
        },
      },
    },
  })
  @UseInterceptors(
    FileInterceptor('file', {
      storage: diskStorage({
        destination: './uploads',
        filename: (req, file, cb) => {
          const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
          cb(null, `${uniqueSuffix}${path.extname(file.originalname)}`);
        },
      }),
      fileFilter: (req, file, cb) => {
        if (path.extname(file.originalname).toLowerCase() !== '.csv') {
          return cb(new Error('Only CSV files are allowed'), false);
        }
        cb(null, true);
      },
    }),
  )
  async analyzeCsvFile(@UploadedFile() file: Express.Multer.File): Promise<AnalysisResult> {
    return await this.analyzerService.analyzeCsvFile(file.path);
  }
}
