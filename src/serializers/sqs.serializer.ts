import type { Serializer } from '@nestjs/microservices';

export interface SqsSerializedMessage {
  pattern: string;
  data: unknown;
  id?: string;
}

export class SqsSerializer implements Serializer<unknown, string> {
  serialize(packet: SqsSerializedMessage): string {
    return JSON.stringify({
      pattern: packet.pattern,
      data: packet.data,
      id: packet.id,
    });
  }
}
