import { NestFactory } from '@nestjs/core';
import { SwaggerModule, DocumentBuilder } from '@nestjs/swagger';
import { AppModule } from './app.module';
import { Logger } from '@nestjs/common';

async function bootstrap() {
  const logger = new Logger('Bootstrap');
  const port = parseInt(process.env.PORT || '3000', 10);
  
  const app = await NestFactory.create(AppModule);
  
  // Включаем корректную обработку shutdown хуков
  app.enableShutdownHooks();
  
  const config = new DocumentBuilder()
    .setTitle('BDB CRM API')
    .setDescription('API документация для BDB CRM системы')
    .setVersion('1.0')
    .build();
    
  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('api/docs', app, document, {
    customSiteTitle: 'BDB CRM API Документация',
    customJs: [
      'https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui-bundle.min.js',
    ],
    customCssUrl: [
      'https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui.min.css',
    ],
    swaggerOptions: {
      docExpansion: 'none',
      filter: true,
      showRequestDuration: true,
      defaultModelRendering: 'model',
      defaultModelsExpandDepth: 2,
      defaultModelExpandDepth: 2,
      displayRequestDuration: true,
      syntaxHighlight: {
        theme: 'monokai'
      },
      languageUrl: "https://raw.githubusercontent.com/swagger-api/swagger-ui/master/src/core/plugins/swagger-js/configs/lang/ru.json"
    },
  });

  // Добавляем обработчики для сигналов завершения
  process.on('SIGINT', async () => {
    logger.log('Получен сигнал SIGINT. Закрываем приложение...');
    await app.close();
    process.exit(0);
  });
  
  process.on('SIGTERM', async () => {
    logger.log('Получен сигнал SIGTERM. Закрываем приложение...');
    await app.close();
    process.exit(0);
  });

  // Просто запускаем на указанном порту (Heroku сам управляет портами)
  await app.listen(port);
  logger.log(`Приложение запущено на порту: ${port}`);
}

bootstrap().catch(err => {
  console.error('Ошибка при запуске приложения:', err);
  process.exit(1);
});