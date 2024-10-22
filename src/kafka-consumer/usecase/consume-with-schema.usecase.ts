
import { Injectable, Logger } from '@nestjs/common';
import { KafkaMessage } from 'kafkajs';
import Avro from 'avsc';
import { KafkaConsumerLog, KafkaGroup, KafkaSteps} from '../../common/types/kafka-log';
import { ProducerEvent } from '../../kafka-producer/dto/kafka.dto';
import { SchemaService } from '../../common/services/schema/schema.service';
import PublishWithSchemaUseCase from '../../kafka-producer/usecase/publish-with-schema.usecase';
import { parseHeaders } from '../../common/functions/helperFunction';
import 'dotenv/config';

@Injectable()
export default class ConsumeWithSchemaUseCase {
//Temporary Hardcoded Schema
//Possible File System Implementation
private type = Avro.Type.forSchema(
{
    "type": "record",
    "name": "Address",
    "namespace": "collection.test",
    "fields": [
        {
            "name": "Country",
            "type": "string"
        },
        {
            "name": "City",
            "type": "string"
        }
    ]
}
);

constructor(private schemaService: SchemaService,
            private publishWithSchemaUseCase: PublishWithSchemaUseCase,    
) {}
  async consumeEvent(topic: string, partition: number, message: KafkaMessage): Promise<boolean> {
    let consumeReturn: boolean = true;
    let schema: any;
    
    let schemadId: any = message.headers['avro-schema-id'];
    try {
        schema =  await this.schemaService.getEventSchema(schemadId); //Call Schema Regitry
    } 
    catch (error) {
        Logger.error(
            new KafkaConsumerLog(
                topic,
                message.offset.toString(),
                partition.toString(),
                KafkaSteps.GetSchemaFromRegistry,
                KafkaGroup.ConsumeEventWithSchema,
                { 
                    data : {
                        value : message.value.toString(),
                        headers : parseHeaders(message.headers)
                    },
                    error: error.message
                }
            )
        );

        return false;
    }

    let messageValue: any;
    let avroType: any = Avro.Type.forSchema(schema); //Initialize Avro Schema Object

    if (!avroType.isValid(avroType.fromBuffer(message.value as any))) { //VALIDATION if Value Conforms to the Schema
        Logger.error(
            new KafkaConsumerLog(
                topic,
                message.offset.toString(),
                partition.toString(),
                KafkaSteps.ValidateSchema,
                KafkaGroup.ConsumeEventWithSchema,
                { 
                    data : {
                        value : message.value.toString(),
                        headers : parseHeaders(message.headers)
                    },
                    error: 'Schema Validation Error'
                }
                
            )
        );
        return false;
    }
    else 
        messageValue = await avroType.fromBuffer(message.value as any); //DeSerialize

    let eventMessageParsed : any = {
        partition: partition,
        event_offset: message.offset,
        timestamp: new Date(Number(message.timestamp)).toString(),
        key: message.key?.toString(),
        headers: message.headers?.toString(),
        value: messageValue
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
            headers: { 'avro-schema-id' : process.env.KAFKA_PRODUCER_EVENT_SCHEMA }, //Requirement for Publishing with Schema
            value: messageValue
        }
    };

    try {
        consumeReturn =  await this.publishWithSchemaUseCase.publishEvent(event);
        //console.log(`publishWithSchema Result: `, consumeReturn);
        Logger.log(
            new KafkaConsumerLog(
                topic,
                message.offset.toString(),
                partition.toString(),
                KafkaSteps.CallProducer,
                KafkaGroup.ConsumeEventWithSchema,
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
                KafkaGroup.ConsumeEventWithSchema,
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