import { IsNotEmpty, IsString, MinLength } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class CreateDatabaseDto {
    @ApiProperty({
        description: 'Database name',
        example: 'Base clients',
        minLength: 2
    })
    @IsNotEmpty()
    @IsString()
    @MinLength(2)
    name: string;
}