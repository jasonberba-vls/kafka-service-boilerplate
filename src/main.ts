import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import rTracer from 'cls-rtracer';
import helmet from 'helmet';
import compression from 'compression';
import { AppExceptionFilter } from './common/filters/app-exception.filter';
import { CustomLoggerService } from './common/logger/custom-logger.service';
import { HttpStatus, Logger, RequestMethod } from '@nestjs/common';
import { optionsMiddleware } from './common/middlewares/options.middleware';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';

async function bootstrap() {
  
  // == Initiate Microservices == //
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.TCP,
    options: {
      host: 'localhost',
      port: process.env.PORT as any, // PORT is defined for scaling testing in Local. Else defaults to 3000
    },
  });
  // const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {});
  await app.listen();
  app.useLogger(app.get(CustomLoggerService));
  Logger.log(`Running on http://localhost:${process.env.PORT}`);
}
bootstrap();
