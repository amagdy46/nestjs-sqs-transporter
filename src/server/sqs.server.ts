import type { Message, QueueAttributeName } from '@aws-sdk/client-sqs';
import { Logger } from '@nestjs/common';
import {
  type CustomTransportStrategy,
  type IncomingEvent,
  type IncomingRequest,
  Server,
} from '@nestjs/microservices';
import { connectable, isObservable, lastValueFrom, Subject } from 'rxjs';
import { Consumer } from 'sqs-consumer';
import {
  NO_EVENT_HANDLER,
  SQS_DEFAULT_BATCH_SIZE,
  SQS_DEFAULT_VISIBILITY_TIMEOUT,
  SQS_DEFAULT_WAIT_TIME_SECONDS,
} from '../constants';
import { ObservabilityHelper } from '../features/observability';
import { S3LargeMessageHandler } from '../features/s3-large-message';
import type { AWS, SqsServerOptions } from '../interfaces/sqs-options.interface';
import { SqsDeserializer } from '../serializers/sqs.deserializer';
import { SqsContext } from './sqs.context';

export class ServerSqs extends Server implements CustomTransportStrategy {
  // biome-ignore lint/complexity/noBannedTypes: Required by NestJS Server base class interface
  private eventHandlers: Map<string, Function[]> = new Map();
  protected readonly logger = new Logger(ServerSqs.name);
  private consumer: Consumer | null = null;
  private readonly s3Handler?: S3LargeMessageHandler;
  private readonly observability: ObservabilityHelper;

  constructor(private readonly options: SqsServerOptions) {
    super();

    // Set up deserializer
    this.initializeDeserializer({
      deserializer: options.deserializer ?? new SqsDeserializer(),
    });

    // Set up S3 handler if enabled
    if (options.s3LargeMessage?.enabled) {
      this.s3Handler = new S3LargeMessageHandler(options.s3LargeMessage);
    }

    // Set up observability
    this.observability = new ObservabilityHelper(options.observability);
  }

  /**
   * Register event handler (required by NestJS v11+)
   */
  // biome-ignore lint/complexity/noBannedTypes: Required by NestJS Server base class interface
  on<EventKey extends string = string, EventCallback extends Function = Function>(
    event: EventKey,
    callback: EventCallback,
  ): this {
    const handlers = this.eventHandlers.get(event) ?? [];
    handlers.push(callback);
    this.eventHandlers.set(event, handlers);
    return this;
  }

  /**
   * Unwrap the server instance (required by NestJS v11+)
   */
  unwrap<T>(): T {
    return this as unknown as T;
  }

  /**
   * Start listening for messages from SQS
   */
  listen(callback: () => void): void {
    this.consumer = Consumer.create({
      queueUrl: this.options.consumerOptions.queueUrl,
      sqs: this.options.sqs,
      waitTimeSeconds:
        this.options.consumerOptions.waitTimeSeconds ?? SQS_DEFAULT_WAIT_TIME_SECONDS,
      visibilityTimeout:
        this.options.consumerOptions.visibilityTimeout ?? SQS_DEFAULT_VISIBILITY_TIMEOUT,
      batchSize: this.options.consumerOptions.batchSize ?? SQS_DEFAULT_BATCH_SIZE,
      attributeNames: (this.options.consumerOptions.attributeNames ?? [
        'All',
      ]) as QueueAttributeName[],
      messageAttributeNames: this.options.consumerOptions.messageAttributeNames ?? ['All'],
      shouldDeleteMessages: this.options.consumerOptions.shouldDeleteMessages ?? true,
      handleMessage: this.handleMessage.bind(this),
    });

    this.consumer.on('error', (err) => {
      this.logger.error('SQS Consumer error', err.stack);
      this.observability.logError('SQS Consumer error', err, 'SqsServer');
    });

    this.consumer.on('processing_error', (err) => {
      this.logger.error('SQS Processing error', err.stack);
      this.observability.logError('SQS Processing error', err, 'SqsServer');
    });

    this.consumer.on('started', () => {
      this.logger.log('SQS Consumer started');
      this.observability.log('SQS Consumer started', 'SqsServer');
    });

    this.consumer.on('stopped', () => {
      this.logger.log('SQS Consumer stopped');
      this.observability.log('SQS Consumer stopped', 'SqsServer');
    });

    this.consumer.start();
    callback();
  }

  /**
   * Stop the SQS consumer
   */
  close(): void {
    if (this.consumer) {
      this.consumer.stop();
      this.consumer = null;
    }
  }

  /**
   * Handle an incoming SQS message following NestJS microservices pattern
   */
  private async handleMessage(message: Message): Promise<void> {
    const startTime = Date.now();

    return this.observability.createSpan(
      {
        name: 'sqs.handleMessage',
        attributes: {
          'sqs.message_id': message.MessageId ?? 'unknown',
          'sqs.queue_url': this.options.consumerOptions.queueUrl,
        },
      },
      async () => {
        try {
          // Unwrap S3 large message if needed
          let body = message.Body ?? '';
          if (this.s3Handler) {
            body = await this.s3Handler.unwrapIfPointer(body);
          }

          // Create a modified message with unwrapped body
          const unwrappedMessage: AWS.SQS.Message = {
            ...message,
            Body: body,
          };

          // Deserialize the message
          const deserializedMessage = this.deserializer.deserialize(unwrappedMessage) as
            | IncomingRequest
            | IncomingEvent;

          const pattern = deserializedMessage.pattern;
          const data = deserializedMessage.data;

          // Create the context
          const context = new SqsContext([unwrappedMessage, pattern]);

          // Get the handler using NestJS Server base class method
          const handler = this.getHandlerByPattern(pattern);

          if (!handler) {
            this.logger.warn(`${NO_EVENT_HANDLER} Pattern: ${JSON.stringify(pattern)}`);
            return;
          }

          // Check if this is an event handler (@EventPattern) or message handler (@MessagePattern)
          if (handler.isEventHandler) {
            // Use NestJS handleEvent for @EventPattern - fire-and-forget
            await this.handleEvent(pattern, { pattern, data }, context);
          } else {
            // For @MessagePattern (request-response), we'd need response handling
            // SQS is typically fire-and-forget, but support it for completeness
            const response$ = this.transformToObservable(await handler(data, context));
            // For SQS, we don't send responses back, just await completion
            if (response$) {
              await lastValueFrom(response$);
            }
          }

          // Record success metric
          const duration = Date.now() - startTime;
          this.observability.recordMetric('sqs.message.processed', 1, {
            pattern,
            status: 'success',
          });
          this.observability.recordMetric('sqs.message.duration', duration, {
            pattern,
          });
        } catch (error) {
          const err = error as Error;
          this.logger.error(`Error processing message: ${err.message}`, err.stack);
          this.observability.logError('Error processing message', err, 'SqsServer');

          // Record error metric
          const duration = Date.now() - startTime;
          this.observability.recordMetric('sqs.message.processed', 1, {
            status: 'error',
          });
          this.observability.recordMetric('sqs.message.duration', duration, {
            status: 'error',
          });

          throw error;
        }
      },
    ) as Promise<void>;
  }

  /**
   * Handle event pattern messages following NestJS Server pattern.
   * This properly integrates with NestJS lifecycle hooks.
   */
  public async handleEvent(
    pattern: string,
    packet: { pattern: string; data: unknown },
    context: SqsContext,
  ): Promise<unknown> {
    const handler = this.getHandlerByPattern(pattern);

    if (!handler) {
      return this.logger.error(`${NO_EVENT_HANDLER} Event pattern: ${JSON.stringify(pattern)}.`);
    }

    const resultOrStream = await handler(packet.data, context);

    // Handle Observable responses properly following NestJS pattern
    if (isObservable(resultOrStream)) {
      const connectableSource = connectable(resultOrStream, {
        connector: () => new Subject(),
        resetOnDisconnect: false,
      });
      connectableSource.connect();
      await lastValueFrom(connectableSource);
    }

    return resultOrStream;
  }
}
