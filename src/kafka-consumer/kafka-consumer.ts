import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';
import 'dotenv/config';
import ConsumeUseCase from './usecase/consume.usecase';
import ConsumeWithSchemaUseCase from './usecase/consume-with-schema.usecase';
import { KafkaGroup, KafkaConsumerLog, KafkaSteps } from '../common/types/kafka-log';
import { CustomPartitionAssigner } from './partitioner/partition-assigner';

@Injectable()
export class KafkaConsumer implements OnModuleInit, OnModuleDestroy {
  private kafka: Kafka;
  private consumer: Consumer;

  constructor(
      private consumeUseCase : ConsumeUseCase,
      private consumeWithSchemaUseCase : ConsumeWithSchemaUseCase
  ) {
    this.kafka = new Kafka({
      clientId: 'my-app-1', // Define for Custom Partitioner
      brokers: [process.env.KAFKA_BROKER], // Kafka broker address

      // AUTHENTICATION
      //ssl: true,
      sasl: {
        //mechanism: 'scram-sha-512', // For AWS
        mechanism: 'plain', // For LOCAL Testing
        username: process.env.KAFKA_CONSUMER_USERNAME,
        password: process.env.KAFKA_CONSUMER_PASSWORD
      },
    });
    //this.consumer = this.kafka.consumer({ groupId: process.env.KAFKA_CONSUMER_GROUP });
    //Custom Partitioner
    this.consumer = this.kafka.consumer({ groupId: process.env.KAFKA_CONSUMER_GROUP, partitionAssigners: [CustomPartitionAssigner('test_topic_001', 'my-app-')] });
  }

  async onModuleInit() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: 'test_topic_001' }); // Replace with your topic
    await this.consumer.run({
        autoCommit: false, //FLAG to Manually Commit Offset
        eachMessage: async ({ topic, partition, message }) => {
          let consumeSuccessful : boolean = false;
          let schemadId: any = message.headers['avro-schema-id'];

          if (schemadId) {
            consumeSuccessful = await this.consumeWithSchemaUseCase.consumeEvent(topic, partition, message);
          }
          else {
            consumeSuccessful = await this.consumeUseCase.consumeEvent(topic, partition, message);
          }

          if (!consumeSuccessful) {
            Logger.error(
              new KafkaConsumerLog(
                  topic,
                  message.offset.toString(),
                  partition.toString(),
                  KafkaSteps.ConsumeEvent,
                  KafkaGroup.ConsumeRun,
                  'Consume Failed - Stopping Consumer.'
              )
            );
            this.consumerStop(); //Failsafe line
          }
          else {
            //OFFSET COMMIT
            let nextOffset = Number(message.offset) + 1;
            try {
              await this.consumer.commitOffsets([
                { 
                  topic: topic, 
                  partition: partition, 
                  offset: nextOffset.toString()},
              ]);
              Logger.log(
                new KafkaConsumerLog(
                    topic,
                    message.offset.toString(),
                    partition.toString(),
                    KafkaSteps.CommitOffset,
                    KafkaGroup.ConsumeRun,
                    `OFFSET Committed: ${nextOffset}`
                )
              );
              //console.log(`OFFSET Committed: `, nextOffset);
              //await new Promise(resolve => setTimeout(resolve, 1000)); //DELAY Execution for Monitoring
            }
            catch (error) {
              Logger.error(
                new KafkaConsumerLog(
                    topic,
                    message.offset.toString(),
                    partition.toString(),
                    KafkaSteps.CommitOffset,
                    KafkaGroup.ConsumeRun,
                    { 
                        data : {
                            offset: nextOffset.toString()
                          },
                        error: error.message
                    }
                    
                )
              );
              this.consumerStop();
            }
          }
        },
    });
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();
  }

  async consumerStop(){
    console.log('consumerStop START');
    await this.consumer.disconnect();
    console.log('consumerStop END');
    process.exit(0);
  }
}
