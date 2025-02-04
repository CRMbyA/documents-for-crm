import { ImportResultDto } from './import-result.dto';
import { ApiProperty } from '@nestjs/swagger';

export class ContinueImportResultDto extends ImportResultDto {
    @ApiProperty({
        description: 'Номер последней обработанной строки',
        example: 15000,
        type: Number
    })
    lastProcessedLine: number;

    @ApiProperty({
        description: 'Номер строки, с которой начался импорт',
        example: 10000,
        type: Number
    })
    startFromLine: number;

    @ApiProperty({
        description: 'Статус выполнения импорта',
        example: 'completed',
        enum: ['completed', 'timeout'],
        type: String
    })
    status: 'completed' | 'timeout';
}