export interface AnalysisResult {
  totalRows: number;
  columns: string[];
  preview: any[];
  columnsInfo: {
    [key: string]: {
      filled: number;
      empty: number;
    }
  };
}
