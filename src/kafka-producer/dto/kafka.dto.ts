import { ProducerRecord, Message } from 'kafkajs';

export class ProducerEvent {
    topic: string
    messages: EventMessage
}

export class EventMessage implements Message {
    key: Buffer | string | null
    value: Buffer | string | null
    headers?: any
    partition?: number
}
