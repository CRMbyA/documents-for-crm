import { ApiProperty } from '@nestjs/swagger';
import { DatabaseType } from '../types/database.types';

export class CreateDatabaseDto {
  @ApiProperty({ description: 'Название базы данных' })
  name: string;

  @ApiProperty({ description: 'Описание базы данных', required: false })
  description?: string;

  @ApiProperty({ description: 'Строка подключения к базе данных' })
  connectionString: string;

  @ApiProperty({ description: 'Тип базы данных', enum: DatabaseType })
  type: DatabaseType;
}