import { Controller, Post, Req } from '@nestjs/common';
import { KafkaProducerService } from './kakfa-producer.service';
import { Request } from 'express';
import { ProducerEvent } from './dto/kafka.dto';
import PublishWithSchemaUseCase from './usecase/publish-with-schema.usecase';

@Controller('kafka-producer')
export class KafkaProducerController {
  constructor(private kafkaProducerService: KafkaProducerService,
              private publishWithSchemaUseCase: PublishWithSchemaUseCase
  ) {}

  @Post('publish')
  async publishEvent(@Req() req: Request) {
    let event : ProducerEvent = req.body;
    return await this.kafkaProducerService.publishEvent(event);
  }

  @Post('publish-with-schema')
  async publishEventWithSchema(@Req() req: Request) {
    let event : ProducerEvent = req.body;
    return await this.publishWithSchemaUseCase.publishEvent(event);
  }
}
 