import type { MessageAttributeValue } from '@aws-sdk/client-sqs';
import { Logger } from '@nestjs/common';
import { ClientProxy, type ReadPacket, type WritePacket } from '@nestjs/microservices';
import { Producer, type Message as ProducerMessage } from 'sqs-producer';
import { v4 as uuidv4 } from 'uuid';
import { CORRELATION_ID_HEADER, MESSAGE_PATTERN_HEADER } from '../constants';
import { DeduplicationHelper } from '../features/deduplication';
import { ObservabilityHelper } from '../features/observability';
import { S3LargeMessageHandler } from '../features/s3-large-message';
import type { SqsClientOptions } from '../interfaces/sqs-options.interface';
import { SqsSerializer } from '../serializers/sqs.serializer';

export class ClientSqs extends ClientProxy {
  protected readonly logger = new Logger(ClientSqs.name);
  private readonly producer: Producer;
  private readonly s3Handler?: S3LargeMessageHandler;
  private readonly observability: ObservabilityHelper;
  private readonly sqsSerializer: SqsSerializer;
  private connected = false;

  constructor(private readonly options: SqsClientOptions) {
    super();

    // Create the sqs-producer instance
    this.producer = Producer.create({
      queueUrl: options.queueUrl,
      sqs: options.sqs,
    });

    // Set up serializer
    this.sqsSerializer = (options.serializer as SqsSerializer) ?? new SqsSerializer();

    // Set up S3 handler if enabled
    if (options.s3LargeMessage?.enabled) {
      this.s3Handler = new S3LargeMessageHandler(options.s3LargeMessage);
    }

    // Set up observability
    this.observability = new ObservabilityHelper(options.observability);
  }

  /**
   * Connect to SQS (no-op for SQS as it's stateless)
   */
  async connect(): Promise<void> {
    this.connected = true;
    this.logger.log('SQS Client connected');
    this.observability.log('SQS Client connected', 'SqsClient');
  }

  /**
   * Close connection (no-op for SQS)
   */
  close(): void {
    this.connected = false;
    this.logger.log('SQS Client closed');
    this.observability.log('SQS Client closed', 'SqsClient');
  }

  /**
   * Unwrap the client instance (required by NestJS v11+)
   */
  unwrap<T>(): T {
    return this as unknown as T;
  }

  /**
   * Publish a message and wait for response (request-response pattern)
   * Note: This requires a response queue setup which is more complex.
   * For now, this will just send and not wait for response.
   */
  protected publish(
    packet: ReadPacket<unknown>,
    callback: (packet: WritePacket<unknown>) => void,
  ): () => void {
    const correlationId = uuidv4();

    this.sendMessage(packet.pattern, packet.data, correlationId)
      .then(() => {
        // For fire-and-forget, just acknowledge
        callback({ response: { success: true } });
      })
      .catch((err) => {
        callback({ err });
      });

    return () => {
      // Cleanup function - nothing to clean up for SQS
    };
  }

  /**
   * Dispatch an event (fire-and-forget pattern)
   */
  protected async dispatchEvent<T = void>(packet: ReadPacket<unknown>): Promise<T> {
    return this.observability.createSpan(
      {
        name: 'sqs.dispatchEvent',
        attributes: {
          'sqs.pattern': packet.pattern,
          'sqs.queue_url': this.options.queueUrl,
        },
      },
      async () => {
        await this.sendMessage(packet.pattern, packet.data);
        return undefined as T;
      },
    ) as Promise<T>;
  }

  /**
   * Send a message to SQS using sqs-producer
   */
  private async sendMessage(
    pattern: string,
    data: unknown,
    correlationId?: string,
  ): Promise<string> {
    const startTime = Date.now();
    const messageId = correlationId ?? uuidv4();

    try {
      // Serialize the message
      let body = this.sqsSerializer.serialize({
        pattern,
        data,
        id: messageId,
      });

      // Wrap large messages with S3 if needed
      if (this.s3Handler) {
        body = await this.s3Handler.wrapIfLarge(body);
      }

      // Build message attributes
      const messageAttributes: Record<string, MessageAttributeValue> = {
        [MESSAGE_PATTERN_HEADER]: {
          DataType: 'String',
          StringValue: pattern,
        },
      };

      if (correlationId) {
        messageAttributes[CORRELATION_ID_HEADER] = {
          DataType: 'String',
          StringValue: correlationId,
        };
      }

      // Build the producer message
      const producerMessage: ProducerMessage = {
        id: messageId,
        body,
        messageAttributes,
      };

      // Add FIFO-specific attributes if enabled
      if (this.options.fifo?.enabled) {
        // Determine message group ID
        if (typeof this.options.fifo.messageGroupId === 'function') {
          producerMessage.groupId = this.options.fifo.messageGroupId(pattern, data);
        } else {
          producerMessage.groupId = this.options.fifo.messageGroupId ?? pattern;
        }

        // Determine deduplication ID
        if (this.options.fifo.contentBasedDeduplication !== true) {
          if (this.options.fifo.deduplicationId) {
            producerMessage.deduplicationId = this.options.fifo.deduplicationId(pattern, data);
          } else {
            producerMessage.deduplicationId = DeduplicationHelper.contentBasedDeduplicationId({
              pattern,
              data,
            });
          }
        }
      }

      // Send the message using sqs-producer
      const results = await this.producer.send([producerMessage]);
      const resultMessageId = results[0]?.MessageId ?? messageId;

      // Record success metric
      const duration = Date.now() - startTime;
      this.observability.recordMetric('sqs.message.sent', 1, {
        pattern,
        status: 'success',
      });
      this.observability.recordMetric('sqs.message.send_duration', duration, {
        pattern,
      });

      this.logger.debug(`Message sent to SQS: ${resultMessageId}`);

      return resultMessageId;
    } catch (error) {
      const err = error as Error;
      this.logger.error(`Error sending message to SQS: ${err.message}`, err.stack);
      this.observability.logError('Error sending message to SQS', err, 'SqsClient');

      // Record error metric
      const duration = Date.now() - startTime;
      this.observability.recordMetric('sqs.message.sent', 1, {
        pattern,
        status: 'error',
      });
      this.observability.recordMetric('sqs.message.send_duration', duration, {
        pattern,
        status: 'error',
      });

      throw error;
    }
  }
}
