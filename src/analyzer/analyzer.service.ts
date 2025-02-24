import { Injectable, Logger } from '@nestjs/common';
import * as csv from 'csv-parser';
import * as fs from 'fs';
import { AnalysisResult } from './types/analysis.types';

@Injectable()
export class AnalyzerService {
  private readonly logger = new Logger(AnalyzerService.name);
  private readonly MAX_ROWS = 100;
  private readonly PREVIEW_ROWS = 3;
  private readonly LOG_FREQUENCY = 10; // Логировать каждые 10 строк

  async analyzeCsvFile(filePath: string): Promise<AnalysisResult> {
    const result: AnalysisResult = {
      totalRows: 0,
      columns: [],
      preview: [],
      columnsInfo: {}
    };

    this.logger.log(`Начинаем анализ файла: ${filePath}`);

    return new Promise<AnalysisResult>((resolve, reject) => {
      const readStream = fs.createReadStream(filePath);
      const parser = csv();

      readStream
        .pipe(parser)
        .on('headers', (headers) => {
          result.columns = headers;
          headers.forEach(header => {
            result.columnsInfo[header] = { filled: 0, empty: 0 };
          });
        })
        .on('data', (data) => {
          result.totalRows++;
          
          if (result.totalRows <= this.MAX_ROWS) { // Изменено с < на <=
            if (result.preview.length < this.PREVIEW_ROWS) {
              result.preview.push(data);
            }
            
            Object.entries(data).forEach(([column, value]) => {
              if (value?.toString().trim()) {
                result.columnsInfo[column].filled++;
              } else {
                result.columnsInfo[column].empty++;
              }
            });

            // Логируем прогресс каждые LOG_FREQUENCY строк
            if (result.totalRows % this.LOG_FREQUENCY === 0) {
              this.logger.log(
                `Прогресс: обработано ${result.totalRows} строк ` +
                `(${Math.min(100, Math.round(result.totalRows / this.MAX_ROWS * 100))}%)`
              );
            }

            // Перемещаем проверку достижения лимита сюда
            if (result.totalRows === this.MAX_ROWS) {
              this.logger.log(`Достигнут лимит анализа: ${this.MAX_ROWS} строк`);
              readStream.destroy();
              resolve(result); // Добавляем resolve здесь
            }
          }
        })
        .on('end', () => {
          // Обрабатываем случай, когда файл короче MAX_ROWS
          if (result.totalRows < this.MAX_ROWS) {
            this.logger.log(
              `Анализ завершен. Всего строк: ${result.totalRows}`
            );
            resolve(result);
          }
        })
        .on('error', (error) => {
          this.logger.error(`Ошибка при анализе файла: ${error.message}`);
          reject(error);
        });
    });
  }
}
