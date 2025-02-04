import { ApiProperty } from "@nestjs/swagger";

export class DatabaseResponseDto {
    @ApiProperty({
        description: 'ID of the database',
        example: '123e4567-e89b-12d3-a456-426614174000'
    })
    id: string;

    @ApiProperty({
        description: 'Name of the database',
        example: 'Database 2024'
    })
    name: string;

    @ApiProperty({
        description: 'Number of clients in the database',
        example: 1000000,
        required: false
    })
    clientsCount?: number;
}