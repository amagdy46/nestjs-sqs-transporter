import type { S3Client } from "@aws-sdk/client-s3";
import type { SQSClient } from "@aws-sdk/client-sqs";
import type { LoggerService } from "@nestjs/common";
import type { Deserializer, Serializer } from "@nestjs/microservices";
import type { S3PointerFormat } from "../constants";

export interface S3LargeMessageOptions {
	enabled: boolean;
	s3Client: S3Client;
	bucket: string;
	threshold?: number;
	keyPrefix?: string;
	/**
	 * Format for S3 pointers:
	 * - 'aws-extended': AWS Extended Client format [{ "@class": "...", s3BucketName, s3Key }]
	 * - 'simple': Simple format { __s3pointer: { bucket, key } }
	 * - 'auto': Auto-detect when reading (supports both), uses 'simple' when writing
	 * @default 'auto'
	 */
	pointerFormat?: S3PointerFormat;
	/**
	 * Custom key name for the S3 pointer in 'simple' format.
	 * Only used when pointerFormat is 'simple' or 'auto'.
	 * @default '__s3pointer'
	 * @example 's3Pointer' -> { s3Pointer: { bucket, key } }
	 */
	pointerKey?: string;
}

export interface ObservabilityOptions {
	tracing?: boolean;
	metrics?: boolean;
	logging?: {
		logger: LoggerService;
		level: "debug" | "info" | "warn" | "error";
	};
}

export interface SqsServerOptions {
	sqs: SQSClient;
	consumerOptions: {
		queueUrl: string;
		waitTimeSeconds?: number;
		visibilityTimeout?: number;
		batchSize?: number;
		attributeNames?: string[];
		messageAttributeNames?: string[];
		shouldDeleteMessages?: boolean;
	};
	s3LargeMessage?: S3LargeMessageOptions;
	observability?: ObservabilityOptions;
	serializer?: Serializer;
	deserializer?: Deserializer;
	/**
	 * Custom key name for the message pattern field.
	 * Use 'type' if your messages use { type: 'EVENT_NAME', ... } format.
	 * @default 'pattern'
	 * @example 'type' -> reads pattern from { type: 'ORDER_CREATED', data: {...} }
	 */
	patternKey?: string;
	/**
	 * Timeout in milliseconds to wait for in-flight messages during graceful shutdown.
	 * @default 30000 (30 seconds)
	 */
	shutdownTimeoutMs?: number;
}

export interface SqsClientOptions {
	sqs: SQSClient;
	queueUrl: string;
	s3LargeMessage?: S3LargeMessageOptions;
	observability?: ObservabilityOptions;
	serializer?: Serializer;
	deserializer?: Deserializer;
	fifo?: {
		enabled: boolean;
		contentBasedDeduplication?: boolean;
		messageGroupId?: string | ((pattern: string, data: unknown) => string);
		deduplicationId?: (pattern: string, data: unknown) => string;
	};
	/**
	 * Custom key name for the message pattern field.
	 * Use 'type' if your messages use { type: 'EVENT_NAME', ... } format.
	 * @default 'pattern'
	 * @example 'type' -> sends as { type: 'ORDER_CREATED', data: {...} }
	 */
	patternKey?: string;
}

// Re-export for convenience
export namespace AWS {
	export namespace SQS {
		export interface Message {
			MessageId?: string;
			ReceiptHandle?: string;
			MD5OfBody?: string;
			Body?: string;
			Attributes?: Record<string, string>;
			MD5OfMessageAttributes?: string;
			MessageAttributes?: Record<
				string,
				{
					StringValue?: string;
					BinaryValue?: Uint8Array;
					DataType?: string;
				}
			>;
		}
	}
}
