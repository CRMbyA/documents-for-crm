import { ApiProperty } from '@nestjs/swagger';

export class ColumnStats {
  @ApiProperty({ description: 'Количество непустых значений' })
  nonEmpty: number;

  @ApiProperty({ description: 'Количество пустых значений' })
  empty: number;

  @ApiProperty({ description: 'Количество уникальных значений' })
  uniqueCount?: number;

  uniqueValues?: Set<string>;
}

export class CsvAnalysisResult {
  @ApiProperty({ description: 'Общее количество строк в файле' })
  totalRows: number;

  @ApiProperty({ description: 'Количество проанализированных строк' })
  analyzedRows: number;

  @ApiProperty({ description: 'Список колонок' })
  columns: string[];

  @ApiProperty({ description: 'Статистика по колонкам' })
  columnStats: { [key: string]: ColumnStats };

  @ApiProperty({ description: 'Примеры данных (первые 5 строк)' })
  sampleData: any[];
}
