import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { DataSource, Repository } from 'typeorm';
import { Database } from '../entities/database.entity';
import { CreateDatabaseDto } from '../dto/create-database.dto';
import { DatabaseResponseDto } from '../dto/database.response.dto';
import { DatabaseClient } from '../entities/database-client.entity';

@Injectable()
export class DatabaseService {
    constructor(
        @InjectRepository(Database)
        private databaseRepository: Repository<Database>,
        private dataSource: DataSource
    ) {}

    async create(createDatabaseDto: CreateDatabaseDto): Promise<DatabaseResponseDto> {
        const database = this.databaseRepository.create(createDatabaseDto);
        const saved = await this.databaseRepository.save(database);
        
        return {
            id: saved.id,
            name: saved.name,
            clientsCount: 0
        };
    }

    async findOne(id: string): Promise<DatabaseResponseDto> {
        const query = `
            SELECT 
                d.id as "id",
                d.name as "name",
                COUNT(dc.id) as "clientsCount"
            FROM "database" d
            LEFT JOIN "database_client" dc ON dc."databaseId" = d.id
            WHERE d.id = $1
            GROUP BY d.id, d.name
        `;

        const result = await this.dataSource.query(query, [id]);

        if (!result.length) {
            throw new NotFoundException('Database not found');
        }

        return {
            id: result[0].id,
            name: result[0].name,
            clientsCount: parseInt(result[0].clientsCount) || 0
        };
    }

    async findAll(): Promise<DatabaseResponseDto[]> {
        const query = `
            SELECT 
                d.id as "id",
                d.name as "name",
                COUNT(dc.id) as "clientsCount"
            FROM "database" d
            LEFT JOIN "database_client" dc ON dc."databaseId" = d.id
            GROUP BY d.id, d.name
        `;

        const databases = await this.dataSource.query(query);

        return databases.map(db => ({
            id: db.id,
            name: db.name,
            clientsCount: parseInt(db.clientsCount) || 0
        }));
    }

    async getDatabaseStats(id: string): Promise<{
        totalRecords: number,
        databaseName: string,
        lastUpdated?: Date
    }> {
        const query = `
            SELECT 
                d.name as "databaseName",
                COUNT(dc.id) as "totalRecords",
                MAX(dc.created_at) as "lastUpdated"
            FROM "database" d
            LEFT JOIN "database_client" dc ON dc."databaseId" = d.id
            WHERE d.id = $1
            GROUP BY d.id, d.name
        `;

        const result = await this.dataSource.query(query, [id]);

        if (!result.length) {
            throw new NotFoundException('Database not found');
        }

        return {
            databaseName: result[0].databaseName,
            totalRecords: parseInt(result[0].totalRecords) || 0,
            lastUpdated: result[0].lastUpdated ? new Date(result[0].lastUpdated) : undefined
        };
    }

    async findClientsByPhone(phone: string): Promise<Array<{
        id: string;
        full_name: string;
        phone: string;
        email: string;
        database_name: string;
        database_id: string;
    }>> {
        // ������� ����� �������� �� ���� �������� ����� ����
        const cleanPhone = phone.replace(/\D/g, '');
        
        const query = `
            SELECT 
                dc.id as "id",
                dc.full_name as "full_name",
                dc.phone as "phone",
                dc.email as "email",
                d.name as "database_name",
                d.id as "database_id"
            FROM "database_client" dc
            JOIN "database" d ON dc."databaseId" = d.id
            WHERE REPLACE(dc.phone, '+', '') LIKE $1
            OR REPLACE(dc.phone, '-', '') LIKE $1
            OR REPLACE(dc.phone, ' ', '') LIKE $1
            OR REPLACE(REPLACE(REPLACE(dc.phone, '+', ''), '-', ''), ' ', '') LIKE $1
            ORDER BY RANDOM()
            LIMIT 1
        `;

        // ��������� wildcards ��� ���������� ����������
        const searchPattern = `%${cleanPhone}%`;
        const results = await this.dataSource.query(query, [searchPattern]);

        return results.map(result => ({
            id: result.id,
            full_name: result.full_name,
            phone: result.phone,
            email: result.email,
            database_name: result.database_name,
            database_id: result.database_id
        }));
    }

    async remove(id: string): Promise<void> {
        const queryRunner = this.dataSource.createQueryRunner();
        await queryRunner.connect();
        await queryRunner.startTransaction();

        try {
            const exists = await queryRunner.manager
                .createQueryBuilder(Database, 'database')
                .where('database.id = :id', { id })
                .getExists();

            if (!exists) {
                throw new NotFoundException('Database not found');
            }

            // TypeORM will handle the cascade delete based on the entity relationships
            await queryRunner.manager
                .createQueryBuilder()
                .delete()
                .from(Database)
                .where('id = :id', { id })
                .execute();

            await queryRunner.commitTransaction();
        } catch (err) {
            await queryRunner.rollbackTransaction();
            throw err;
        } finally {
            await queryRunner.release();
        }
    }
}