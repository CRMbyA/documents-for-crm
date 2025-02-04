import { ApiProperty } from '@nestjs/swagger';

export class ImportResultDto {
    @ApiProperty({
        description: 'Lenth of the client list',
        example: 1000000
    })
    totalProcessed: number;

    @ApiProperty({
        description: 'Count of successfully imported records',
        example: 999000
    })
    successCount: number;

    @ApiProperty({
        description: 'Count of records with errors',
        example: 1000
    })
    errorCount: number;

    @ApiProperty({
        description: 'Message about import result',
        example: 'Import completed successfully'
    })
    message: string;
}
