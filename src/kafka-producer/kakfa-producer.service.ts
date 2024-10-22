import { Inject, Injectable, Logger } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { catchError, map } from 'rxjs/operators';
import { from, Observable, throwError } from 'rxjs';
import { ProducerEvent } from './dto/kafka.dto';
import { InternalApiWrapper } from '../common/wrapper/internalApiWrapper';
import { ConfigService } from '@nestjs/config';
import { KafkaGroup, KafkaProducerLog, KafkaSteps } from '../common/types/kafka-log';
import { parseHeaders } from '../common/functions/helperFunction';

@Injectable()
export class KafkaProducerService {
  private SCHEMA_API_URL: string;
  constructor(@Inject('KAFKA_SERVICE') private client: ClientProxy,
                private internalApiWrapper: InternalApiWrapper, 
                private configService: ConfigService) {
    this.SCHEMA_API_URL = this.configService.get('SCHEMA_API_URL');
  }

  async publishEvent(event: ProducerEvent):  Promise<any> {
    let returnValue = await from(this.client.emit(event.topic, event.messages)).pipe(
      map(() => {
        //console.log('Event published SUCCESS!');
        return true;
      }),
      catchError(err => {
          Logger.error(
            new KafkaProducerLog(
                event.topic,
                event.messages.key?.toString(),
                event.messages.partition?.toString(),
                KafkaSteps.KafkaPublishEvent,
                KafkaGroup.PublishToKafkaServer,
                { 
                    data : {
                        value : event.messages.value.toString(),
                        headers : parseHeaders(event.messages.headers)
                    },
                    error: err.message
                }
                
            )
        );
        return [false]; // Return an observable with false
      }),
    ).toPromise();
    return returnValue;
  }
}
