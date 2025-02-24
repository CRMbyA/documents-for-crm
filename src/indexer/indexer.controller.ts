import { Controller, Post, Get, Param, Sse } from '@nestjs/common';
import { ApiTags, ApiOperation } from '@nestjs/swagger';
import { IndexerService } from './indexer.service';
import * as path from 'path';
import { Observable, Subject } from 'rxjs';
import { map } from 'rxjs/operators';
import { DatabaseStatsResponse } from './types/index.types';

@ApiTags('Индексация')
@Controller('indexer')
export class IndexerController {
  constructor(private readonly indexerService: IndexerService) {}

  @Post(':filename')
  @ApiOperation({ summary: 'Создать индекс для CSV файла' })
  async createIndex(@Param('filename') filename: string) {
    // Ensure filename is a string and sanitize it
    const sanitizedFilename = path.basename(filename);
    const filePath = path.join('./uploads', sanitizedFilename);
    const databaseId = path.basename(sanitizedFilename, '.csv');

    return await this.indexerService.createIndex(filePath, {
      databaseId,
      phoneColumn: 'cmainphonenum',
      partitionSize: 100
    });
  }

  @Sse('search-stream/:phone')
  @ApiOperation({ summary: 'Потоковый поиск по номеру телефона с прогрессом' })
  searchPhoneStream(@Param('phone') phone: string): Observable<any> {
    const subject = new Subject();

    this.indexerService.findByPhoneWithProgress(phone, (progress) => {
      // Отправляем каждое обновление как есть
      subject.next({ data: progress });

      // Закрываем поток только когда поиск действительно завершен
      if (progress.isComplete) {
        setTimeout(() => subject.complete(), 100);
      }
    });

    return subject.asObservable();
  }

  @Get('search/:phone')
  @ApiOperation({ summary: 'Быстрый поиск по номеру телефона' })
  async findByPhone(@Param('phone') phone: string) {
    return await this.indexerService.findByPhoneParallel(phone);
  }

  @Get('stats')
  @ApiOperation({ summary: 'Получить статистику по всем базам данных' })
  async getDatabaseStats(): Promise<DatabaseStatsResponse> {
    return await this.indexerService.getDatabaseStats();
  }

  @Get('stats/:databaseId')
  @ApiOperation({ summary: 'Получить статистику конкретной базы данных' })
  async getSingleDatabaseStats(@Param('databaseId') databaseId: string) {
    return await this.indexerService.getSingleDatabaseStats(databaseId);
  }
}
