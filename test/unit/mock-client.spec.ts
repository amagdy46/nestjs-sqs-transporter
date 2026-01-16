import { firstValueFrom } from 'rxjs';
import { beforeEach, describe, expect, it } from 'vitest';
import { MockClientSqs } from '../../src/testing/mock-sqs-client';

describe('MockClientSqs', () => {
  let client: MockClientSqs;

  beforeEach(() => {
    client = new MockClientSqs();
  });

  describe('send', () => {
    it('should record sent messages', async () => {
      await firstValueFrom(client.send('TEST_PATTERN', { data: 'test' }));

      const messages = client.getSentMessages();
      expect(messages).toHaveLength(1);
      expect(messages[0].pattern).toBe('TEST_PATTERN');
      expect(messages[0].data).toEqual({ data: 'test' });
    });

    it('should return success response', async () => {
      const result = await firstValueFrom(client.send('TEST_PATTERN', { data: 'test' }));

      expect(result).toMatchObject({ success: true });
    });
  });

  describe('emit', () => {
    it('should record emitted events', async () => {
      client.emit('EVENT_PATTERN', { event: 'data' });

      // Give it a tick to process
      await new Promise((resolve) => setTimeout(resolve, 0));

      const events = client.getEmittedEvents();
      expect(events).toHaveLength(1);
      expect(events[0].pattern).toBe('EVENT_PATTERN');
      expect(events[0].data).toEqual({ event: 'data' });
    });
  });

  describe('getMessagesByPattern', () => {
    it('should filter messages by pattern', async () => {
      await firstValueFrom(client.send('PATTERN_A', { a: 1 }));
      await firstValueFrom(client.send('PATTERN_B', { b: 2 }));
      await firstValueFrom(client.send('PATTERN_A', { a: 3 }));

      const patternA = client.getMessagesByPattern('PATTERN_A');
      const patternB = client.getMessagesByPattern('PATTERN_B');

      expect(patternA).toHaveLength(2);
      expect(patternB).toHaveLength(1);
    });
  });

  describe('getEventsByPattern', () => {
    it('should filter events by pattern', async () => {
      client.emit('EVENT_A', { a: 1 });
      client.emit('EVENT_B', { b: 2 });
      client.emit('EVENT_A', { a: 3 });

      await new Promise((resolve) => setTimeout(resolve, 0));

      const eventsA = client.getEventsByPattern('EVENT_A');
      const eventsB = client.getEventsByPattern('EVENT_B');

      expect(eventsA).toHaveLength(2);
      expect(eventsB).toHaveLength(1);
    });
  });

  describe('clear', () => {
    it('should clear all recorded messages and events', async () => {
      await firstValueFrom(client.send('PATTERN', { data: 1 }));
      client.emit('EVENT', { data: 2 });

      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(client.getSentMessages()).toHaveLength(1);
      expect(client.getEmittedEvents()).toHaveLength(1);

      client.clear();

      expect(client.getSentMessages()).toHaveLength(0);
      expect(client.getEmittedEvents()).toHaveLength(0);
    });
  });

  describe('expectMessageSent', () => {
    it('should return message when found', async () => {
      await firstValueFrom(client.send('FIND_ME', { found: true }));

      const message = client.expectMessageSent('FIND_ME');

      expect(message).toBeDefined();
      expect(message?.data).toEqual({ found: true });
    });

    it('should return undefined when not found', () => {
      const message = client.expectMessageSent('NOT_FOUND');

      expect(message).toBeUndefined();
    });
  });

  describe('expectEventEmitted', () => {
    it('should return event when found', async () => {
      client.emit('FIND_EVENT', { found: true });

      await new Promise((resolve) => setTimeout(resolve, 0));

      const event = client.expectEventEmitted('FIND_EVENT');

      expect(event).toBeDefined();
      expect(event?.data).toEqual({ found: true });
    });

    it('should return undefined when not found', () => {
      const event = client.expectEventEmitted('NOT_FOUND');

      expect(event).toBeUndefined();
    });
  });
});
