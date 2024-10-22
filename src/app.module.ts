import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { APP_GUARD, APP_INTERCEPTOR } from '@nestjs/core';
import joi from 'joi';
import { LoggerModule } from './common/logger/logger.module';
import { PostStatusInterceptor } from './common/interceptors/post-status.interceptor';
import { HealthModule } from './common/health/health.module';
import { KafkaConsumerModule } from './kafka-consumer/kakfa-consumer.module';
import { KafkaProducerModule } from './kafka-producer/kakfa-producer.module';

const configValidationSchema = joi.object({
  NODE_ENV: joi
    .string()
    .valid('development', 'test', 'sandbox', 'live')
    .required(),
  PORT: joi.number().required().default(3000),
  LOG_DIR: joi.string().required(),
  LOG_FILENAME: joi.string().required(),
});

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      validationSchema: configValidationSchema,
      envFilePath:'.env'
    }),
    LoggerModule,
    HealthModule,
    KafkaConsumerModule,
    KafkaProducerModule,
  ],
  providers: [
    {
      provide: APP_INTERCEPTOR,
      useClass: PostStatusInterceptor,
    },
  ],
})
export class AppModule {}
