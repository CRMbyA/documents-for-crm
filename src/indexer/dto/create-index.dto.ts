
import { ApiProperty } from '@nestjs/swagger';

export class CreateIndexDto {
  @ApiProperty({ description: 'ID базы данных' })
  databaseId: string;

  @ApiProperty({ description: 'Название колонки с телефонами' })
  phoneColumn: string;

  @ApiProperty({ description: 'Размер одной партиции (по умолчанию 100)' })
  partitionSize?: number;
}