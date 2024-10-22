import { Injectable, Logger } from '@nestjs/common';
import { KafkaProducerLog, KafkaGroup, KafkaSteps} from '../../common/types/kafka-log';
import { KafkaProducerService } from '../kakfa-producer.service';
import Avro from 'avsc';
import { ProducerEvent } from '../dto/kafka.dto';
import { SchemaService }  from '../../common/services/schema/schema.service';
import { parseHeaders } from '../../common/functions/helperFunction';

@Injectable()
export default class PublishWithSchemaUseCase {
constructor(
    private kafkaProducerService: KafkaProducerService,
    private schemaService: SchemaService
) {}
  async publishEvent(event: ProducerEvent): Promise<any> {

    let schemadId: any = event.messages.headers['avro-schema-id'];
    if (!schemadId) { 
        Logger.error(
            new KafkaProducerLog(
                event.topic,
                event.messages.key?.toString(),
                event.messages.partition?.toString(),
                KafkaSteps.FetchSchemaIdFromHeader,
                KafkaGroup.PublishEventWithSchema,
                { 
                    data : event.messages?.value,
                    error: 'Invalid SchemaId'
                }
            )
        );

        return false;
    }
  
    let schema: any;
    try {
        schema =  await this.schemaService.getEventSchema(schemadId); //Call Schema Regitry
    } 
    catch (error) {
        Logger.error(
            new KafkaProducerLog(
                event.topic,
                event.messages.key?.toString(),
                event.messages.partition?.toString(),
                KafkaSteps.GetSchemaFromRegistry,
                KafkaGroup.PublishEventWithSchema,
                { 
                    data : {
                        value : event.messages.value.toString(),
                        headers : parseHeaders(event.messages.headers)
                    },
                    error: error.message
                }
            )
        );

        return false;
    }

    let avroType: any = Avro.Type.forSchema(schema); //Initialize Avro Schema Object
    if (!avroType.isValid(event.messages.value)) { //VALIDATION if Value Conforms to the Schema
        Logger.error(
            new KafkaProducerLog(
                event.topic,
                event.messages.key?.toString(),
                event.messages.partition?.toString(),
                KafkaSteps.ValidateSchema,
                KafkaGroup.PublishEventWithSchema,
                { 
                    data : {
                        value : event.messages.value.toString(),
                        headers : parseHeaders(event.messages.headers)
                    },
                    error: 'Schema Validation Error'
                }
                
            )
        );
        return false;
    }
    else 
        event.messages.value = await avroType.toBuffer(event.messages.value); // Serialize

    return await this.kafkaProducerService.publishEvent(event);
  }
}
