import { NestFactory } from '@nestjs/core';
import { SwaggerModule, DocumentBuilder } from '@nestjs/swagger';
import { AppModule } from './app.module';
import { Logger } from '@nestjs/common';
import { execSync } from 'child_process';

/**
 * Функция для освобождения порта в Windows
 * @param port номер порта для освобождения
 * @returns возвращает true если порт был освобожден или свободен, false если не удалось освободить
 */
function releasePort(port: number): boolean {
  const logger = new Logger('PortRelease');
  try {
    // Находим PID процесса, использующего порт
    const findCommand = `netstat -ano | findstr :${port} | findstr LISTENING`;
    const output = execSync(findCommand, { encoding: 'utf-8' });
    
    // Извлекаем PID из вывода команды
    const pidMatch = output.match(/(\d+)$/m);
    
    if (pidMatch && pidMatch[1]) {
      const pid = pidMatch[1];
      logger.log(`Найден процесс (PID: ${pid}) использующий порт ${port}. Завершаем...`);
      
      // Завершаем процесс
      execSync(`taskkill /F /PID ${pid}`);
      logger.log(`Процесс завершен. Порт ${port} освобожден.`);
      return true;
    } else {
      logger.log(`Процесс, использующий порт ${port}, не найден. Порт свободен.`);
      return true;
    }
  } catch (error) {
    // Если исключение связано с тем, что процесс не найден (код 1), значит порт свободен
    if (error.status === 1) {
      logger.log(`Порт ${port} свободен.`);
      return true;
    }
    
    logger.error(`Ошибка при освобождении порта ${port}: ${error.message}`);
    return false;
  }
}

async function bootstrap() {
  const logger = new Logger('Bootstrap');
  const port = parseInt(process.env.PORT || '3000', 10);
  
  // Пытаемся освободить порт перед запуском сервера
  if (!releasePort(port)) {
    logger.warn(`Не удалось освободить порт ${port}. Возможно потребуется ручное вмешательство.`);
  }
  
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

  // Функция для попытки запуска на разных портах
  const startServer = async (portToUse: number, maxAttempts = 3): Promise<void> => {
    try {
      await app.listen(portToUse);
      logger.log(`Приложение запущено на порту: ${portToUse}`);
    } catch (error) {
      if (error.code === 'EADDRINUSE' && maxAttempts > 0) {
        const newPort = portToUse + 1;
        logger.warn(`Порт ${portToUse} всё ещё используется несмотря на попытку освобождения. Пробуем порт ${newPort}...`);
        
        // Попытка освободить новый порт перед использованием
        releasePort(newPort);
        await startServer(newPort, maxAttempts - 1);
      } else {
        logger.error(`Не удалось запустить сервер: ${error.message}`);
        throw error;
      }
    }
  };

  // Запускаем сервер на порту
  await startServer(port);
}

bootstrap().catch(err => {
  console.error('Ошибка при запуске приложения:', err);
  process.exit(1);
});