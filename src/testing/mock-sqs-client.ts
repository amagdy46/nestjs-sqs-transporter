import { ClientProxy, type ReadPacket, type WritePacket } from '@nestjs/microservices';

export interface SentMessage {
  pattern: string;
  data: unknown;
  timestamp: Date;
}

/**
 * Mock SQS Client for testing purposes.
 * Records sent messages for verification without actual SQS connection.
 */
export class MockClientSqs extends ClientProxy {
  private sentMessages: SentMessage[] = [];
  private emittedEvents: SentMessage[] = [];

  async connect(): Promise<void> {
    // No-op
  }

  close(): void {
    // No-op
  }

  unwrap<T>(): T {
    return this as unknown as T;
  }

  protected publish(
    packet: ReadPacket<unknown>,
    callback: (packet: WritePacket<unknown>) => void,
  ): () => void {
    this.sentMessages.push({
      pattern: packet.pattern,
      data: packet.data,
      timestamp: new Date(),
    });

    callback({ response: { success: true, messageId: `mock-${Date.now()}` } });

    return () => {};
  }

  protected async dispatchEvent<T = void>(packet: ReadPacket<unknown>): Promise<T> {
    this.emittedEvents.push({
      pattern: packet.pattern,
      data: packet.data,
      timestamp: new Date(),
    });
    return undefined as T;
  }

  /**
   * Get all messages sent via send()
   */
  getSentMessages(): SentMessage[] {
    return [...this.sentMessages];
  }

  /**
   * Get all events emitted via emit()
   */
  getEmittedEvents(): SentMessage[] {
    return [...this.emittedEvents];
  }

  /**
   * Get messages filtered by pattern
   */
  getMessagesByPattern(pattern: string): SentMessage[] {
    return this.sentMessages.filter((m) => m.pattern === pattern);
  }

  /**
   * Get events filtered by pattern
   */
  getEventsByPattern(pattern: string): SentMessage[] {
    return this.emittedEvents.filter((m) => m.pattern === pattern);
  }

  /**
   * Clear all recorded messages
   */
  clear(): void {
    this.sentMessages = [];
    this.emittedEvents = [];
  }

  /**
   * Assert that a message was sent with the given pattern
   */
  expectMessageSent(pattern: string): SentMessage | undefined {
    return this.sentMessages.find((m) => m.pattern === pattern);
  }

  /**
   * Assert that an event was emitted with the given pattern
   */
  expectEventEmitted(pattern: string): SentMessage | undefined {
    return this.emittedEvents.find((m) => m.pattern === pattern);
  }
}
