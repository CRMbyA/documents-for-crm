import { Injectable, Logger } from '@nestjs/common';
import * as csv from 'csv-parser';
import * as fs from 'fs';
import * as iconv from 'iconv-lite';
import { AnalysisResult, EncodingType } from './types/analysis.types';
import * as chardet from 'chardet';
import * as path from 'path';

@Injectable()
export class AnalyzerService {
  private readonly logger = new Logger(AnalyzerService.name);
  private readonly MAX_ROWS = 100;
  private readonly PREVIEW_ROWS = 3;
  private readonly LOG_FREQUENCY = 10;
  private readonly SAMPLE_SIZE = 4096; // Размер выборки для определения кодировки (4KB)

  /**
   * Анализирует CSV/TSV-файл с поддержкой различных кодировок
   * @param filePath Путь к файлу
   * @param encoding Кодировка файла (auto, utf8, windows1251, koi8r, iso88595)
   */
  async analyzeCsvFile(
    filePath: string,
    encoding: EncodingType = 'auto'
  ): Promise<AnalysisResult> {
    const result: AnalysisResult = {
      totalRows: 0,
      columns: [],
      preview: [],
      columnsInfo: {},
      encoding: encoding
    };

    this.logger.log(`Начинаем анализ файла: ${filePath}`);
    
    // Получаем информацию о файле
    try {
      const fileStats = fs.statSync(filePath);
      this.logger.log(`Размер файла: ${this.formatFileSize(fileStats.size)}`);
      
      // Определяем тип файла по расширению
      const fileExt = path.extname(filePath).toLowerCase();
      this.logger.log(`Расширение файла: ${fileExt}`);
    } catch (error) {
      this.logger.error(`Ошибка при получении информации о файле: ${error.message}`);
      throw error;
    }
    
    // Определяем кодировку файла, если установлено 'auto'
    if (encoding === 'auto') {
      try {
        // Читаем только начало файла для определения кодировки
        const fd = fs.openSync(filePath, 'r');
        const buffer = Buffer.alloc(this.SAMPLE_SIZE);
        fs.readSync(fd, buffer, 0, this.SAMPLE_SIZE, 0);
        fs.closeSync(fd);
        
        const detectedEncoding = await this.detectEncoding(buffer);
        result.encoding = detectedEncoding;
        this.logger.log(`Автоматически определена кодировка: ${detectedEncoding}`);
      } catch (error) {
        this.logger.error(`Ошибка при определении кодировки: ${error.message}`);
        result.encoding = 'utf8'; // Используем UTF-8 по умолчанию в случае ошибки
      }
    }

    // Определяем разделитель на основе анализа содержимого
    let delimiter = ','; // По умолчанию запятая для CSV
    
    try {
      // Читаем первые несколько строк файла для определения разделителя
      const fd = fs.openSync(filePath, 'r');
      const buffer = Buffer.alloc(1024);
      const bytesRead = fs.readSync(fd, buffer, 0, 1024, 0);
      fs.closeSync(fd);
      
      const sample = buffer.slice(0, bytesRead).toString();
      
      // Определяем, какой разделитель чаще встречается
      const tabCount = (sample.match(/\t/g) || []).length;
      const commaCount = (sample.match(/,/g) || []).length;
      const semicolonCount = (sample.match(/;/g) || []).length;
      
      this.logger.log(`Обнаружено символов: табуляций=${tabCount}, запятых=${commaCount}, точек с запятой=${semicolonCount}`);
      
      if (tabCount > commaCount && tabCount > semicolonCount) {
        delimiter = '\t';
        this.logger.log('Определен разделитель: табуляция');
      } else if (semicolonCount > commaCount) {
        delimiter = ';';
        this.logger.log('Определен разделитель: точка с запятой');
      } else {
        this.logger.log('Определен разделитель: запятая');
      }
    } catch (error) {
      this.logger.warn(`Ошибка при определении разделителя: ${error.message}. Используем стандартный разделитель.`);
    }

    return new Promise<AnalysisResult>((resolve, reject) => {
      try {
        // Создаем поток чтения файла с увеличенным буфером
        const readStream = fs.createReadStream(filePath, {
          highWaterMark: 64 * 1024 // 64KB буфер
        });
        
        // Конвертируем кодировку, если это не UTF-8
        let streamToUse: NodeJS.ReadableStream = readStream;
        
        if (result.encoding !== 'utf8') {
          const encodingMap: Record<string, string> = {
            'windows1251': 'win1251',
            'koi8r': 'koi8-r',
            'iso88595': 'iso-8859-5'
          };
          const iconvEncoding = encodingMap[result.encoding || 'utf8'] || result.encoding;
          streamToUse = readStream.pipe(iconv.decodeStream(iconvEncoding as string));
        }
        
        // Настраиваем парсер CSV/TSV с оптимальными опциями
        const parserOptions = {
          separator: delimiter,
          escape: '"',
          quote: '"',
          skipLines: 0,
          headers: true,
          skipComments: true
        };
        
        this.logger.log(`Начинаем парсинг с разделителем: "${delimiter === '\t' ? '\\t' : delimiter}"`);
        const parser = csv(parserOptions);

        streamToUse
          .pipe(parser)
          .on('headers', (headers) => {
            this.logger.log(`Обнаружены заголовки: ${headers.length} колонок`);
            
            // Очищаем заголовки от кавычек и лишних символов
            const cleanedHeaders = headers.map(header => 
              header.replace(/^["']+|["']+$/g, '').trim()
            );
            
            result.columns = cleanedHeaders;
            cleanedHeaders.forEach(header => {
              result.columnsInfo[header] = { filled: 0, empty: 0 };
            });
          })
          .on('data', (data) => {
            result.totalRows++;
            
            if (result.totalRows <= this.MAX_ROWS) {
              if (result.preview.length < this.PREVIEW_ROWS) {
                result.preview.push(data);
              }
              
              Object.entries(data).forEach(([column, value]) => {
                // Очищаем имя колонки от кавычек
                const cleanColumn = column.replace(/^["']+|["']+$/g, '').trim();
                
                if (value?.toString().trim()) {
                  if (result.columnsInfo[cleanColumn]) {
                    result.columnsInfo[cleanColumn].filled++;
                  } else if (result.columnsInfo[column]) {
                    result.columnsInfo[column].filled++;
                  }
                } else {
                  if (result.columnsInfo[cleanColumn]) {
                    result.columnsInfo[cleanColumn].empty++;
                  } else if (result.columnsInfo[column]) {
                    result.columnsInfo[column].empty++;
                  }
                }
              });
              
              // Логируем прогресс каждые LOG_FREQUENCY строк
              if (result.totalRows % this.LOG_FREQUENCY === 0) {
                this.logger.log(
                  `Прогресс: обработано ${result.totalRows} строк ` +
                  `(${Math.min(100, Math.round(result.totalRows / this.MAX_ROWS * 100))}%)`
                );
              }
              
              // Проверяем достижение лимита
              if (result.totalRows === this.MAX_ROWS) {
                this.logger.log(`Достигнут лимит анализа: ${this.MAX_ROWS} строк`);
                readStream.destroy();
                resolve(result);
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
            this.logger.error(`Ошибка при парсинге файла: ${error.message}`);
            
            // Пробуем с другим разделителем, если возникла ошибка парсинга
            if (delimiter === ',' && !result.columns.length) {
              this.logger.log('Пробуем парсить с разделителем табуляция...');
              readStream.destroy();
              
              // Перезапускаем с другим разделителем
              this.analyzeCsvFileWithDelimiter(filePath, result.encoding || 'utf8', '\t')
                .then(newResult => resolve(newResult))
                .catch(err => reject(err));
            } else {
              reject(error);
            }
          });
      } catch (error) {
        this.logger.error(`Непредвиденная ошибка при анализе: ${error.message}`);
        reject(error);
      }
    });
  }

  /**
   * Вспомогательный метод для анализа с конкретным разделителем
   */
  private async analyzeCsvFileWithDelimiter(
    filePath: string, 
    encoding: EncodingType, 
    delimiter: string
  ): Promise<AnalysisResult> {
    const result: AnalysisResult = {
      totalRows: 0,
      columns: [],
      preview: [],
      columnsInfo: {},
      encoding: encoding
    };
    
    return new Promise<AnalysisResult>((resolve, reject) => {
      try {
        const readStream = fs.createReadStream(filePath, {
          highWaterMark: 64 * 1024
        });
        
        let streamToUse: NodeJS.ReadableStream = readStream;
        
        if (encoding !== 'utf8') {
          const encodingMap: Record<string, string> = {
            'windows1251': 'win1251',
            'koi8r': 'koi8-r',
            'iso88595': 'iso-8859-5'
          };
          const iconvEncoding = encodingMap[encoding || 'utf8'] || encoding;
          streamToUse = readStream.pipe(iconv.decodeStream(iconvEncoding as string));
        }
        
        const parserOptions = {
          separator: delimiter,
          escape: '"',
          quote: '"',
          skipLines: 0,
          headers: true
        };
        
        this.logger.log(`Повторный парсинг с разделителем: "${delimiter === '\t' ? '\\t' : delimiter}"`);
        const parser = csv(parserOptions);
        
        streamToUse
          .pipe(parser)
          .on('headers', (headers) => {
            result.columns = headers;
            headers.forEach(header => {
              result.columnsInfo[header] = { filled: 0, empty: 0 };
            });
          })
          .on('data', (data) => {
            result.totalRows++;
            
            if (result.totalRows <= this.MAX_ROWS) {
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
              
              if (result.totalRows === this.MAX_ROWS) {
                readStream.destroy();
                resolve(result);
              }
            }
          })
          .on('end', () => {
            if (result.totalRows < this.MAX_ROWS) {
              resolve(result);
            }
          })
          .on('error', (error) => {
            reject(error);
          });
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Определяет кодировку файла по содержимому
   * @param buffer Буфер с содержимым файла
   * @returns Тип кодировки
   */
  private async detectEncoding(buffer: Buffer): Promise<EncodingType> {
    try {
      // Используем chardet для определения кодировки
      const detected = await chardet.detect(buffer);
      
      // Преобразуем результат в тип EncodingType
      const detectedStr = String(detected).toLowerCase();
      
      if (detectedStr.includes('windows') || detectedStr.includes('1251')) {
        return 'windows1251';
      }
      if (detectedStr.includes('koi8')) {
        return 'koi8r';
      }
      if (detectedStr.includes('8859-5') || detectedStr.includes('88595')) {
        return 'iso88595';
      }
      
      // Дополнительная проверка на UTF-8 BOM
      if (buffer.length >= 3 && 
          buffer[0] === 0xEF && 
          buffer[1] === 0xBB && 
          buffer[2] === 0xBF) {
        return 'utf8';
      }
      
      return 'utf8';
    } catch (error) {
      this.logger.error(`Ошибка при определении кодировки: ${error.message}`);
      return 'utf8';
    }
  }

  /**
   * Форматирует размер файла в человекочитаемый вид
   */
  private formatFileSize(bytes: number): string {
    if (bytes === 0) return '0 Байт';
    const k = 1024;
    const sizes = ['Байт', 'КБ', 'МБ', 'ГБ', 'ТБ'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }
}