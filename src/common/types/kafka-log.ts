export class KafkaConsumerLog {
    topic: string;
    offset: string;
    partition: string;
    step: string;
    group: KafkaGroup;
    message: any;
  
    constructor(
      topic: string,
      offset: string,
      partition: string,
      step: string,
      group: KafkaGroup,
      message: any
    ) {
      this.topic = topic;
      this.offset = offset;
      this.partition = partition;
      this.step = step;
      this.group = group;
      this.message = message;
    }
  }

export class KafkaProducerLog {
    topic: string;
    key: string;
    partition: string;
    step: string;
    group: KafkaGroup;
    message: any;
  
    constructor(
      topic: string,
      key: string,
      partition: string,
      step: string,
      group: KafkaGroup,
      message: any
    ) {
      this.topic = topic;
      this.key = key;
      this.partition = partition;
      this.step = step;
      this.group = group;
      this.message = message;
    }
  }

  export enum KafkaGroup {
    PublishEventWithSchema = 'Publish Event With Schema',
    ConsumeEventWithSchema = 'Consume Event With Schema',
    ConsumeEvent = 'Consume Event',
    ConsumeRun = 'Consume Run',
    PublishToKafkaServer = 'Publish To Kafka Server',
  }

  export enum KafkaSteps {
    FetchSchemaIdFromHeader = 'Fetch SchemaId From Header',
    GetSchemaFromRegistry = 'Get Schema From Registry',
    ValidateSchema = 'Validate Schema',
    CallProducer = 'Call Producer',
    KafkaPublishEvent = 'Kafka Publish Event',
    CommitOffset = 'Commit Offset',
    ConsumeEvent = "Consume Event",
  }
  