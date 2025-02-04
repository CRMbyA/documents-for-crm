import { ApiProperty } from '@nestjs/swagger';

export class DatabaseStatsDto {
    @ApiProperty({
        description: 'Database name',
        example: 'Client Database 2024'
    })
    databaseName: string;

    @ApiProperty({
        description: 'Total number of records',
        example: 1000000
    })
    totalRecords: number;

    @ApiProperty({
        description: 'Last record update date',
        example: '2024-02-04T12:00:00Z',
        required: false
    })
    lastUpdated?: Date;
}