export type User = {
    id: string;
    name: string;
    email: string;
    createdAt: Date;
    updatedAt: Date;
};

export type Product = {
    id: string;
    name: string;
    description: string;
    price: number;
    createdAt: Date;
    updatedAt: Date;
};

export type Order = {
    id: string;
    userId: string;
    productIds: string[];
    totalAmount: number;
    createdAt: Date;
    updatedAt: Date;
};

export interface Database {
    id: string;
    name: string;
    description?: string;
    connectionString: string;
    type: DatabaseType;
    tables: Table[];
    createdAt: Date;
}

export interface Table {
    name: string;
    columns: Column[];
    rowCount?: number;
}

export interface Column {
    name: string;
    type: ColumnType;
    nullable: boolean;
    isPrimaryKey?: boolean;
}

export enum DatabaseType {
    POSTGRES = 'postgres',
    MYSQL = 'mysql',
    MONGODB = 'mongodb',
    SQLITE = 'sqlite'
}

export enum ColumnType {
    STRING = 'string',
    NUMBER = 'number',
    BOOLEAN = 'boolean',
    DATE = 'date',
    JSON = 'json',
    ARRAY = 'array'
}
