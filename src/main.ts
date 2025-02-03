import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';

async function bootstrap() {
  process.env.TZ = 'Europe/Kiev';
  
  const app = await NestFactory.create(AppModule);
  app.setGlobalPrefix('api')
  app.enableCors();
  
  const config = new DocumentBuilder()
    .setTitle('CRM крупные базы')
    .setDescription('API для управления CRM базами данных')
    .setVersion('2.0')
    .build();
    
  const document = SwaggerModule.createDocument(app, config, {
    deepScanRoutes: true
  });
  
  const customOptions = {
    swaggerOptions: {
      filter: true
    }
  };

  SwaggerModule.setup('api/docs', app, document, customOptions);
  
  await app.listen(process.env.PORT ?? 3000);
}

bootstrap();