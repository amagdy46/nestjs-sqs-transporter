import { type CustomTransportStrategy, Server } from '@nestjs/microservices';
import { isObservable, lastValueFrom } from 'rxjs';
import type { AWS } from '../interfaces/sqs-options.interface';
import { SqsDeserializer } from '../serializers/sqs.deserializer';
import { SqsContext } from '../server/sqs.context';

export interface MockSqsServerOptions {
  onMessage?: (pattern: string, data: unknown) => void;
}

/**
 * Mock SQS Server for testing purposes.
 * Allows simulating message receipt without actual SQS connection.
 */
export class MockServerSqs extends Server implements CustomTransportStrategy {
  private readonly onMessage?: (pattern: string, data: unknown) => void;
  // biome-ignore lint/complexity/noBannedTypes: Required by NestJS Server base class interface
  private eventHandlers: Map<string, Function[]> = new Map();

  constructor(options?: MockSqsServerOptions) {
    super();
    this.onMessage = options?.onMessage;
    this.initializeDeserializer({
      deserializer: new SqsDeserializer(),
    });
  }

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

  unwrap<T>(): T {
    return this as unknown as T;
  }

  listen(callback: () => void): void {
    callback();
  }

  close(): void {
    // No-op
  }

  /**
   * Simulate receiving a message for testing
   */
  async simulateMessage(pattern: string, data: unknown): Promise<unknown> {
    const message: AWS.SQS.Message = {
      MessageId: `test-${Date.now()}`,
      Body: JSON.stringify({ pattern, data }),
      MessageAttributes: {},
    };

    const handler = this.getHandlerByPattern(pattern);

    if (!handler) {
      throw new Error(`No handler found for pattern: ${pattern}`);
    }

    const context = new SqsContext([message, pattern]);

    this.onMessage?.(pattern, data);

    const response = await handler(data, context);

    if (isObservable(response)) {
      return lastValueFrom(response);
    }

    return response;
  }

  /**
   * Check if a handler exists for a pattern
   */
  hasHandler(pattern: string): boolean {
    return this.getHandlerByPattern(pattern) !== undefined;
  }

  /**
   * Get all registered patterns
   */
  getPatterns(): string[] {
    return Array.from(this.messageHandlers.keys());
  }
}
