import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import * as crypto from 'crypto';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  const config = new DocumentBuilder()
    .setTitle('Database Search API')
    .setDescription('API for searching in large databases')
    .setVersion('1.0')
    .addBearerAuth()
    .build();

  const document = SwaggerModule.createDocument(app, config);

  SwaggerModule.setup('api', app, document, {
    customSiteTitle: 'Database Search API',
    customCss: `
      @charset "UTF-8";
      .swagger-ui {
        font-family: Arial, sans-serif;
      }
    `,
    customJs: `
      window.onload = function() {
        const meta = document.createElement('meta');
        meta.setAttribute('charset', 'UTF-8');
        document.head.appendChild(meta);
      }
    `,
    swaggerOptions: {
      docExpansion: 'list',
      filter: true,
      showRequestDuration: true,
      syntaxHighlight: {
        theme: 'monokai'
      }
    }
  });

  app.enableCors();
  await app.listen(process.env.PORT ?? 3000);
}

bootstrap();