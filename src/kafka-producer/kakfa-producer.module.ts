import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { KafkaProducerService } from './kakfa-producer.service';
import { KafkaProducerController } from './kafka-producer.controller';
import 'dotenv/config';
import { InternalApiWrapper } from '../common/wrapper/internalApiWrapper';
import PublishWithSchemaUseCase from './usecase/publish-with-schema.usecase';
import { SchemaService } from '../common/services/schema/schema.service';
import { Partitioners } from 'kafkajs';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_SERVICE',
        transport: Transport.KAFKA,
        options: {
          client: {
            //clientId: process.env.LOG_FILENAME, // Replace with your producer client ID
            clientId: 'my-app',
            brokers: [process.env.KAFKA_BROKER],

            // AUTHENTICATION
            //ssl: true,  // For AWS
            sasl: {
              //mechanism: 'scram-sha-512', // For AWS
              mechanism: 'plain', // For LOCAL Testing
              username: process.env.KAFKA_PRODUCER_USERNAME,
              password: process.env.KAFKA_PRODUCER_PASSWORD
            },

            // requestTimeout: 25000, // Default value is 30000 (30 secs)
            // enforceRequestTimeout: false // Disable requestTimeOut
          },
          producer: {
            createPartitioner: Partitioners.DefaultPartitioner
          }
        },
      },
    ]),
  ],
  providers: [
    KafkaProducerService, 
    PublishWithSchemaUseCase,
    InternalApiWrapper, 
    SchemaService
  ],
  controllers: [KafkaProducerController],
  exports: [KafkaProducerService, PublishWithSchemaUseCase],
})
export class KafkaProducerModule {}