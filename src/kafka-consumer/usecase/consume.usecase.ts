import { Injectable, Logger } from '@nestjs/common';
import { KafkaConsumerLog, KafkaGroup, KafkaSteps} from '../../common/types/kafka-log';
import { KafkaProducerService } from '../../kafka-producer/kakfa-producer.service';
import { KafkaMessage } from 'kafkajs';
import { ProducerEvent } from '../../kafka-producer/dto/kafka.dto';
import { parseHeaders } from '../../common/functions/helperFunction';

@Injectable()
export default class ConsumeUseCase {
constructor(private kafkaProducerService: KafkaProducerService,) {}
  async consumeEvent(topic: string, partition: number, message: KafkaMessage): Promise<boolean> {
    let consumeReturn: boolean = true;

    let eventMessageParsed : any = {
        partition: partition,
        event_offset: message.offset,
        timestamp: new Date(Number(message.timestamp)).toString(),
        key: message.key?.toString(),
        headers: message.headers?.toString(),
        value: message.value.toString()
      };
    console.log(`Topic ${topic} : `, eventMessageParsed);


    //----------------------------------------------
    //          INSERT USE CASE HERE
    //----------------------------------------------


    // #region START Publish New EVENT
    
    let event: ProducerEvent = {
        topic: 'test_topic_002', // Event Topic
        messages: {
            key: message.key?.toString(),
            value: message.value.toString()
        }
      };

    try {
        let consumeReturn =  await this.kafkaProducerService.publishEvent(event);
        //console.log(`publish Result: `, consumeReturn);
        Logger.log(
          new KafkaConsumerLog(
              topic,
              message.offset.toString(),
              partition.toString(),
              KafkaSteps.CallProducer,
              KafkaGroup.ConsumeEvent,
              { 
                  data : {
                      topic: event.topic,
                      value : event.messages.value.toString(),
                      headers : parseHeaders(event.messages.headers)
                  },
                  isPublishSuccessful: consumeReturn
              }
              
          )
      );
    } catch (error) {
        consumeReturn = false;
        Logger.error(
          new KafkaConsumerLog(
              topic,
              message.offset.toString(),
              partition.toString(),
              KafkaSteps.CallProducer,
              KafkaGroup.ConsumeEvent,
              { 
                  data : {
                      topic: event.topic,
                      value : event.messages.value.toString(),
                      headers : parseHeaders(event.messages.headers)
                  },
                  error: error.message
              }
              
          )
        );
    }

    // #endregion END Publish New EVENT
    
    return consumeReturn;
  }
}