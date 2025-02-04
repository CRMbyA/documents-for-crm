import {
    Entity,
    Column,
    PrimaryGeneratedColumn,
    ManyToOne,
    JoinColumn,
    AfterLoad
} from 'typeorm';
import { Database } from './database.entity';
import { Exclude } from 'class-transformer';

@Entity()
export class DatabaseClient {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column()
    full_name: string;

    @Column({ nullable: true })
    email: string;

    @Column({ nullable: true })
    phone: string;

    @Column({ nullable: true })
    address: string;

    @Column({ nullable: true })
    birth_date: string;

    @Column({ nullable: true })
    snils: string;

    @Column({ type: 'text', nullable: true })
    comment: string;

    @Exclude()
    @ManyToOne(() => Database, database => database.clients, {
        onDelete: 'CASCADE'
    })
    @JoinColumn()
    database: Database;

    database_name: string;

    @AfterLoad()
    setDatabaseName() {
        if (this.database) {
            this.database_name = this.database.name;
        }
    }
}