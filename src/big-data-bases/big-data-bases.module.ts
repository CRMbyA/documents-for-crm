import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConfigModule } from '@nestjs/config';
import { MulterModule } from '@nestjs/platform-express';
import { ImportController } from './controllers/import.controller';
import { DatabaseService } from './services/database.service';
import { ImportService } from './services/import.service';
import { Database } from './entities/database.entity';
import { DatabaseClient } from './entities/database-client.entity';

@Module({
    imports: [
        TypeOrmModule.forFeature([Database, DatabaseClient]),
        
        MulterModule.register({
            dest: './uploads',
        }),
        
        ConfigModule
    ],
    controllers: [ImportController],
    providers: [
        DatabaseService,
        ImportService
    ],
    exports: [
        DatabaseService,
        ImportService
    ]
})
export class BigDataBasesModule {}