import { describe, expect, it } from 'vitest';
import { CORRELATION_ID_HEADER, MESSAGE_PATTERN_HEADER } from '../../src/constants';
import { SqsDeserializer } from '../../src/serializers/sqs.deserializer';
import { SqsSerializer } from '../../src/serializers/sqs.serializer';

describe('SqsSerializer', () => {
  const serializer = new SqsSerializer();

  it('should serialize a message with pattern and data', () => {
    const result = serializer.serialize({
      pattern: 'TEST_PATTERN',
      data: { foo: 'bar' },
    });

    const parsed = JSON.parse(result);
    expect(parsed).toEqual({
      pattern: 'TEST_PATTERN',
      data: { foo: 'bar' },
    });
  });

  it('should include correlation id when provided', () => {
    const result = serializer.serialize({
      pattern: 'TEST_PATTERN',
      data: { foo: 'bar' },
      id: 'correlation-123',
    });

    const parsed = JSON.parse(result);
    expect(parsed.id).toBe('correlation-123');
  });

  it('should handle null data', () => {
    const result = serializer.serialize({
      pattern: 'TEST_PATTERN',
      data: null,
    });

    const parsed = JSON.parse(result);
    expect(parsed.data).toBeNull();
  });

  it('should handle complex nested data', () => {
    const complexData = {
      nested: {
        array: [1, 2, 3],
        object: { deep: true },
      },
    };

    const result = serializer.serialize({
      pattern: 'COMPLEX',
      data: complexData,
    });

    const parsed = JSON.parse(result);
    expect(parsed.data).toEqual(complexData);
  });
});

describe('SqsDeserializer', () => {
  const deserializer = new SqsDeserializer();

  it('should deserialize a message with pattern and data in body', () => {
    const message = {
      MessageId: 'msg-123',
      Body: JSON.stringify({
        pattern: 'TEST_PATTERN',
        data: { foo: 'bar' },
      }),
    };

    const result = deserializer.deserialize(message);

    expect(result).toEqual({
      pattern: 'TEST_PATTERN',
      data: { foo: 'bar' },
    });
  });

  it('should use correlation id from message attributes', () => {
    const message = {
      MessageId: 'msg-123',
      Body: JSON.stringify({
        pattern: 'TEST_PATTERN',
        data: { foo: 'bar' },
      }),
      MessageAttributes: {
        [CORRELATION_ID_HEADER]: {
          StringValue: 'corr-456',
          DataType: 'String',
        },
      },
    };

    const result = deserializer.deserialize(message) as {
      id?: string;
      pattern: string;
      data: unknown;
    };

    expect(result.id).toBe('corr-456');
  });

  it('should extract pattern from message attributes when body is raw data', () => {
    const message = {
      MessageId: 'msg-123',
      Body: JSON.stringify({ someData: 'value' }),
      MessageAttributes: {
        [MESSAGE_PATTERN_HEADER]: {
          StringValue: 'CUSTOM_PATTERN',
          DataType: 'String',
        },
      },
    };

    const result = deserializer.deserialize(message);

    expect(result.pattern).toBe('CUSTOM_PATTERN');
    expect(result.data).toEqual({ someData: 'value' });
  });

  it('should handle non-JSON body as raw data', () => {
    const message = {
      MessageId: 'msg-123',
      Body: 'plain text message',
      MessageAttributes: {
        [MESSAGE_PATTERN_HEADER]: {
          StringValue: 'RAW_PATTERN',
          DataType: 'String',
        },
      },
    };

    const result = deserializer.deserialize(message);

    expect(result.pattern).toBe('RAW_PATTERN');
    expect(result.data).toBe('plain text message');
  });

  it('should throw error for empty body', () => {
    const message = {
      MessageId: 'msg-123',
      Body: '',
    };

    expect(() => deserializer.deserialize(message)).toThrow('Message body is empty');
  });

  it('should use unknown pattern when not found', () => {
    const message = {
      MessageId: 'msg-123',
      Body: JSON.stringify({ someData: 'value' }),
    };

    const result = deserializer.deserialize(message);

    expect(result.pattern).toBe('unknown');
  });
});
