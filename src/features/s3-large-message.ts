import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { v4 as uuidv4 } from 'uuid';
import {
  S3_DEFAULT_KEY_PREFIX,
  S3_DEFAULT_THRESHOLD,
  S3_POINTER_CLASS,
  S3_POINTER_KEY,
  type S3PointerFormat,
} from '../constants';
import type { S3LargeMessageOptions } from '../interfaces/sqs-options.interface';

/** AWS Extended Client format (S3 Pointer) */
export interface S3Pointer {
  '@class': string;
  s3BucketName: string;
  s3Key: string;
}

/** Simple format */
export interface SimpleS3Pointer {
  bucket: string;
  key: string;
}

/** Internal normalized pointer */
interface NormalizedPointer {
  bucket: string;
  key: string;
}

export class S3LargeMessageHandler {
  private readonly threshold: number;
  private readonly keyPrefix: string;
  private readonly pointerFormat: S3PointerFormat;
  private readonly pointerKey: string;

  constructor(private readonly options: S3LargeMessageOptions) {
    this.threshold = options.threshold ?? S3_DEFAULT_THRESHOLD;
    this.keyPrefix = options.keyPrefix ?? S3_DEFAULT_KEY_PREFIX;
    this.pointerFormat = options.pointerFormat ?? 'auto';
    this.pointerKey = options.pointerKey ?? S3_POINTER_KEY;
  }

  /**
   * If the message exceeds the threshold, upload to S3 and return a pointer.
   * Otherwise, return the original message.
   */
  async wrapIfLarge(message: string): Promise<string> {
    const messageBytes = Buffer.byteLength(message, 'utf8');

    if (messageBytes <= this.threshold) {
      return message;
    }

    const s3Key = `${this.keyPrefix}${uuidv4()}`;

    await this.options.s3Client.send(
      new PutObjectCommand({
        Bucket: this.options.bucket,
        Key: s3Key,
        Body: message,
        ContentType: 'application/json',
      }),
    );

    return this.createPointerMessage(this.options.bucket, s3Key);
  }

  /**
   * Create a pointer message in the configured format.
   * When 'auto', defaults to 'simple' format for writing (cleaner, no Java baggage).
   */
  private createPointerMessage(bucket: string, key: string): string {
    if (this.pointerFormat === 'aws-extended') {
      const pointer: S3Pointer = {
        '@class': S3_POINTER_CLASS,
        s3BucketName: bucket,
        s3Key: key,
      };
      return JSON.stringify([pointer]);
    }

    // 'simple' or 'auto' - use simple format with custom pointer key
    const pointer: Record<string, SimpleS3Pointer> = {
      [this.pointerKey]: { bucket, key },
    };
    return JSON.stringify(pointer);
  }

  /**
   * If the message is an S3 pointer, download from S3 and return the content.
   * Otherwise, return the original message.
   */
  async unwrapIfPointer(message: string): Promise<string> {
    const pointer = this.parseS3Pointer(message);

    if (!pointer) {
      return message;
    }

    const response = await this.options.s3Client.send(
      new GetObjectCommand({
        Bucket: pointer.bucket,
        Key: pointer.key,
      }),
    );

    const body = await response.Body?.transformToString();
    if (!body) {
      throw new Error(`Failed to read S3 object: ${pointer.key}`);
    }

    return body;
  }

  /**
   * Delete the S3 object after processing (optional cleanup)
   */
  async deleteS3Object(message: string): Promise<void> {
    const pointer = this.parseS3Pointer(message);

    if (!pointer) {
      return;
    }

    await this.options.s3Client.send(
      new DeleteObjectCommand({
        Bucket: pointer.bucket,
        Key: pointer.key,
      }),
    );
  }

  /**
   * Check if the message is an S3 pointer
   */
  isS3Pointer(message: string): boolean {
    return this.parseS3Pointer(message) !== null;
  }

  /**
   * Parse S3 pointer from message, supporting both formats based on configuration.
   * - 'aws-extended': Only parse AWS Extended Client format
   * - 'simple': Only parse simple format
   * - 'auto': Try both formats (auto-detect)
   */
  private parseS3Pointer(message: string): NormalizedPointer | null {
    try {
      const parsed = JSON.parse(message);

      // Try AWS Extended Client format if configured or auto
      if (this.pointerFormat === 'aws-extended' || this.pointerFormat === 'auto') {
        const awsPointer = this.parseAwsExtendedFormat(parsed);
        if (awsPointer) {
          return awsPointer;
        }
      }

      // Try simple format if configured or auto
      if (this.pointerFormat === 'simple' || this.pointerFormat === 'auto') {
        const simplePointer = this.parseSimpleFormat(parsed);
        if (simplePointer) {
          return simplePointer;
        }
      }

      return null;
    } catch {
      return null;
    }
  }

  /**
   * Parse AWS Extended Client format: [{ "@class": "...", s3BucketName, s3Key }]
   */
  private parseAwsExtendedFormat(parsed: unknown): NormalizedPointer | null {
    // Handle array format: [{ "@class": "...", ... }]
    if (Array.isArray(parsed) && parsed.length === 1) {
      const item = parsed[0];
      if (item['@class'] === S3_POINTER_CLASS) {
        return {
          bucket: item.s3BucketName,
          key: item.s3Key,
        };
      }
    }

    // Handle object format: { "@class": "...", ... }
    if (
      typeof parsed === 'object' &&
      parsed !== null &&
      (parsed as Record<string, unknown>)['@class'] === S3_POINTER_CLASS
    ) {
      const obj = parsed as S3Pointer;
      return {
        bucket: obj.s3BucketName,
        key: obj.s3Key,
      };
    }

    return null;
  }

  /**
   * Parse simple format: { pointerKey: { bucket, key } }
   * Uses the configured pointerKey (default: '__s3pointer')
   */
  private parseSimpleFormat(parsed: unknown): NormalizedPointer | null {
    if (typeof parsed !== 'object' || parsed === null) {
      return null;
    }

    const s3Pointer = (parsed as Record<string, unknown>)[this.pointerKey];

    if (typeof s3Pointer !== 'object' || s3Pointer === null) {
      return null;
    }

    const pointer = s3Pointer as Record<string, unknown>;
    if (typeof pointer.bucket === 'string' && typeof pointer.key === 'string') {
      return {
        bucket: pointer.bucket,
        key: pointer.key,
      };
    }

    return null;
  }
}
