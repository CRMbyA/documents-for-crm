// // src/main.ts
// import { NestFactory } from '@nestjs/core';
// import { AppModule } from './app.module';
// import { SwaggerModule, DocumentBuilder } from '@nestjs/swagger';
// import { json, urlencoded } from 'express';

// async function bootstrap() {
//   const app = await NestFactory.create(AppModule);

//   // Увеличиваем лимиты для express
//   app.use(json({ limit: '10gb' }));
//   app.use(urlencoded({ limit: '10gb', extended: true }));

//   const config = new DocumentBuilder()
//     .setTitle('Database Import API')
//     .setDescription('API for importing and managing large client databases')
//     .setVersion('1.0')
//     .build();

//   const document = SwaggerModule.createDocument(app, config);
//   SwaggerModule.setup('api', app, document);

//   await app.listen(3000);
// }
// bootstrap();

import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { SwaggerModule, DocumentBuilder } from '@nestjs/swagger';
import { json, urlencoded } from 'express';


async function bootstrap() {
  process.env.TZ = 'Europe/Kiev';

  const app = await NestFactory.create(AppModule);
  app.setGlobalPrefix('api')
  app.enableCors();

    app.use(json({ limit: '10gb' }));
  app.use(urlencoded({ limit: '10gb', extended: true }));

  const config = new DocumentBuilder()
    .setTitle('Документы для CRM')
    .setDescription('API для управления CRM документами 1.0')
    .setVersion('1.0')
    .build();

  const document = SwaggerModule.createDocument(app, config, {
    deepScanRoutes: true
  });
  
  SwaggerModule.setup('api/docs', app, document);

  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();