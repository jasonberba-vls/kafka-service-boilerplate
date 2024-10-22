import { Module } from '@nestjs/common';
import 'dotenv/config';
import { KafkaConsumer } from './kafka-consumer';
import { KafkaProducerModule } from '../kafka-producer/kakfa-producer.module';
import ConsumeUseCase from './usecase/consume.usecase';
import ConsumeWithSchemaUseCase from './usecase/consume-with-schema.usecase';
import { SchemaService } from '../common/services/schema/schema.service';
import { InternalApiWrapper } from 'src/common/wrapper/internalApiWrapper';

@Module({
  imports: [
    KafkaProducerModule,
  ],
  providers: [
    KafkaConsumer,
    ConsumeUseCase,
    ConsumeWithSchemaUseCase,
    InternalApiWrapper,
    SchemaService
  ],
})
export class KafkaConsumerModule {}