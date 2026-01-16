import { createHash } from 'node:crypto';

/**
 * Generate a content-based deduplication ID from message content.
 * Uses SHA-256 hash of the content.
 */
export function contentBasedDeduplicationId(content: unknown): string {
  const stringContent = typeof content === 'string' ? content : JSON.stringify(content);
  return createHash('sha256').update(stringContent).digest('hex');
}

/**
 * Generate a message group ID for FIFO queues.
 * Used to ensure messages in the same group are processed in order.
 */
export function messageGroupId(groupKey: string): string {
  return groupKey;
}

/**
 * Generate a deduplication ID with a timestamp component.
 * Useful when you want time-based deduplication windows.
 */
export function timedDeduplicationId(content: unknown, windowMs = 300000): string {
  const timeWindow = Math.floor(Date.now() / windowMs);
  const contentHash = contentBasedDeduplicationId(content);
  return `${timeWindow}-${contentHash}`;
}

/**
 * @deprecated Use individual functions instead (contentBasedDeduplicationId, messageGroupId, timedDeduplicationId)
 * Kept for backwards compatibility
 */
export const DeduplicationHelper = {
  contentBasedDeduplicationId,
  messageGroupId,
  timedDeduplicationId,
};
