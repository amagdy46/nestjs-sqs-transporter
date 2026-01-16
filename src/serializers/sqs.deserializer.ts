import type { Deserializer, IncomingEvent, IncomingRequest } from '@nestjs/microservices';
import { CORRELATION_ID_HEADER, MESSAGE_PATTERN_HEADER } from '../constants';
import type { AWS } from '../interfaces/sqs-options.interface';

export interface SqsDeserializedMessage {
  pattern: string;
  data: unknown;
  id?: string;
}

export class SqsDeserializer
  implements Deserializer<AWS.SQS.Message, IncomingRequest | IncomingEvent>
{
  deserialize(message: AWS.SQS.Message): IncomingRequest | IncomingEvent {
    const body = message.Body;
    if (!body) {
      throw new Error('Message body is empty');
    }

    let parsed: SqsDeserializedMessage;

    try {
      parsed = JSON.parse(body);
    } catch {
      // If body is not JSON, treat it as raw data
      // Try to get pattern from message attributes
      const pattern = message.MessageAttributes?.[MESSAGE_PATTERN_HEADER]?.StringValue || 'unknown';
      return {
        pattern,
        data: body,
      };
    }

    // If parsed object has pattern/data structure, use it
    if (parsed.pattern !== undefined) {
      const correlationId =
        message.MessageAttributes?.[CORRELATION_ID_HEADER]?.StringValue || parsed.id;

      return {
        pattern: parsed.pattern,
        data: parsed.data,
        id: correlationId,
      };
    }

    // Otherwise, try to extract pattern from message attributes
    const pattern = message.MessageAttributes?.[MESSAGE_PATTERN_HEADER]?.StringValue || 'unknown';
    return {
      pattern,
      data: parsed,
    };
  }
}
