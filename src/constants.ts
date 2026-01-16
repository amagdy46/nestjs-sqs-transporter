export const SQS_TRANSPORTER = 'SQS_TRANSPORTER';

export const SQS_DEFAULT_WAIT_TIME_SECONDS = 20;
export const SQS_DEFAULT_VISIBILITY_TIMEOUT = 30;
export const SQS_DEFAULT_BATCH_SIZE = 10;

export const S3_DEFAULT_THRESHOLD = 240 * 1024; // 240KB (SQS max is 256KB)
export const S3_DEFAULT_KEY_PREFIX = 'sqs-messages/';
export const S3_POINTER_CLASS = 'software.amazon.payloadoffloading.PayloadS3Pointer';
export const S3_POINTER_KEY = '__s3pointer';

/**
 * S3 pointer format options:
 * - 'aws-extended': AWS Extended Client format [{ "@class": "...", s3BucketName, s3Key }]
 * - 'simple': Simple format { __s3pointer: { bucket, key } }
 * - 'auto': Auto-detect when reading (supports both), uses 'simple' when writing
 */
export type S3PointerFormat = 'aws-extended' | 'simple' | 'auto';

export const CORRELATION_ID_HEADER = 'correlationId';
export const REPLY_TO_HEADER = 'replyTo';
export const MESSAGE_PATTERN_HEADER = 'messagePattern';

export const NO_EVENT_HANDLER = 'There is no matching event handler defined in the remote service.';
