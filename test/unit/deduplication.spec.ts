import { describe, expect, it } from 'vitest';
import { DeduplicationHelper } from '../../src/features/deduplication';

describe('DeduplicationHelper', () => {
  describe('contentBasedDeduplicationId', () => {
    it('should generate consistent hash for same content', () => {
      const content = { foo: 'bar', num: 123 };

      const id1 = DeduplicationHelper.contentBasedDeduplicationId(content);
      const id2 = DeduplicationHelper.contentBasedDeduplicationId(content);

      expect(id1).toBe(id2);
    });

    it('should generate different hashes for different content', () => {
      const content1 = { foo: 'bar' };
      const content2 = { foo: 'baz' };

      const id1 = DeduplicationHelper.contentBasedDeduplicationId(content1);
      const id2 = DeduplicationHelper.contentBasedDeduplicationId(content2);

      expect(id1).not.toBe(id2);
    });

    it('should handle string content directly', () => {
      const content = 'plain string';

      const id = DeduplicationHelper.contentBasedDeduplicationId(content);

      expect(typeof id).toBe('string');
      expect(id.length).toBe(64); // SHA-256 hex length
    });

    it('should handle complex nested objects', () => {
      const content = {
        nested: {
          deep: {
            value: [1, 2, 3],
          },
        },
      };

      const id = DeduplicationHelper.contentBasedDeduplicationId(content);

      expect(typeof id).toBe('string');
      expect(id.length).toBe(64);
    });
  });

  describe('messageGroupId', () => {
    it('should return the group key as-is', () => {
      const groupKey = 'my-group-123';

      const result = DeduplicationHelper.messageGroupId(groupKey);

      expect(result).toBe(groupKey);
    });
  });

  describe('timedDeduplicationId', () => {
    it('should generate different IDs for different time windows', () => {
      const content = { data: 'test' };

      // Use very short window to force different timestamps
      const id1 = DeduplicationHelper.timedDeduplicationId(content, 1);

      // Wait a bit to ensure we're in a different window
      const waitPromise = new Promise((resolve) => setTimeout(resolve, 5));

      return waitPromise.then(() => {
        const id2 = DeduplicationHelper.timedDeduplicationId(content, 1);
        // IDs should be different due to time window change
        // Note: This test might be flaky if execution is too fast
        expect(id1).not.toBe(id2);
      });
    });

    it('should include content hash in the ID', () => {
      const content = { data: 'test' };
      const contentHash = DeduplicationHelper.contentBasedDeduplicationId(content);

      const id = DeduplicationHelper.timedDeduplicationId(content);

      expect(id).toContain(contentHash);
    });
  });
});
