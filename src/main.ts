import { NestFactory } from '@nestjs/core';
import { SwaggerModule, DocumentBuilder } from '@nestjs/swagger';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  
  const config = new DocumentBuilder()
    .setTitle('BDB CRM API')
    .setDescription('API документация для BDB CRM системы')
    .setVersion('1.0')
    .build();
    
  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('api', app, document, {
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

  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
