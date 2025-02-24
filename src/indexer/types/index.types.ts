export interface IndexedData {
  [phoneNumber: string]: {
    data: any;
    partitionKey: number;
  }
}

export interface IndexMetadata {
  id: string;
  originalFileName: string;
  totalRecords: number;
  partitionsCount: number;
  partitionSize: number;
  createdAt: Date;
  phoneColumn: string;
}

export interface Partition {
  id: number;
  data: IndexedData;
  recordsCount: number;
}

export interface DatabaseStats {
  id: string;
  records: number;
  partitions: number;
}

export interface DatabaseStatsResponse {
  totalDatabases: number;
  totalRecords: number;
  databases: DatabaseStats[];
}
