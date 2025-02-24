import { Injectable } from '@nestjs/common';
import { CreateDatabaseDto } from './dto/create-database.dto';
import { Database } from './types/database.types';

@Injectable()
export class DatabaseService {
  private readonly databases: Database[] = [];

  async create(createDatabaseDto: CreateDatabaseDto): Promise<Database> {
    const newDatabase: Database = {
      id: Date.now().toString(),
      ...createDatabaseDto,
      tables: [], // Инициализируем пустым массивом таблиц
      createdAt: new Date(),
    };
    this.databases.push(newDatabase);
    return newDatabase;
  }

  async findAll(): Promise<Database[]> {
    return this.databases;
  }

  async findOne(id: string): Promise<Database | undefined> {
    return this.databases.find(db => db.id === id);
  }

  async remove(id: string): Promise<{ deleted: boolean }> {
    const index = this.databases.findIndex(db => db.id === id);
    if (index > -1) {
      this.databases.splice(index, 1);
    }
    return { deleted: true };
  }
}
