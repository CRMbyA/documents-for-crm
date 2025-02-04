import {
    Entity,
    Column,
    PrimaryGeneratedColumn,
    OneToMany,
    CreateDateColumn
} from 'typeorm';
import { DatabaseClient } from './database-client.entity';

@Entity()
export class Database {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ unique: true })
    name: string;

    @OneToMany(() => DatabaseClient, client => client.database, {
        cascade: true,
        onDelete: 'CASCADE'
    })
    clients: DatabaseClient[];

    @CreateDateColumn()
    created_at: Date;
}