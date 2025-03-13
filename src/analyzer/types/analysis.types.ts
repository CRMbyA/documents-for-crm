
export type EncodingType = 'auto' | 'utf8' | 'windows1251' | 'koi8r' | 'iso88595';

export interface ColumnInfo {
  filled: number;
  empty: number;
}

export interface AnalysisResult {
  totalRows: number;
  columns: string[];
  preview: any[];
  columnsInfo: {
    [key: string]: ColumnInfo;
  };
  encoding?: EncodingType; // Добавлено поле для кодировки
}