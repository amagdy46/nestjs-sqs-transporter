// Client
export { ClientSqs } from "./client/sqs.client";
// Constants
export {
	CORRELATION_ID_HEADER,
	MESSAGE_PATTERN_HEADER,
	REPLY_TO_HEADER,
	S3_DEFAULT_KEY_PREFIX,
	S3_DEFAULT_THRESHOLD,
	S3_POINTER_CLASS,
	S3_POINTER_KEY,
	type S3PointerFormat,
	SQS_DEFAULT_BATCH_SIZE,
	SQS_DEFAULT_VISIBILITY_TIMEOUT,
	SQS_DEFAULT_WAIT_TIME_SECONDS,
	SQS_TRANSPORTER,
} from "./constants";
export {
	contentBasedDeduplicationId,
	DeduplicationHelper,
	messageGroupId,
	timedDeduplicationId,
} from "./features/deduplication";
export { ObservabilityHelper } from "./features/observability";
// Features
export {
	S3LargeMessageHandler,
	type S3Pointer,
	type SimpleS3Pointer,
} from "./features/s3-large-message";
// Interfaces
export type {
	AWS,
	ObservabilityOptions,
	S3LargeMessageOptions,
	SqsClientOptions,
	SqsServerOptions,
} from "./interfaces/sqs-options.interface";
export {
	type SqsDeserializedMessage,
	SqsDeserializer,
} from "./serializers/sqs.deserializer";
// Serializers
export {
	type SqsSerializedMessage,
	SqsSerializer,
} from "./serializers/sqs.serializer";
export { SqsContext } from "./server/sqs.context";
// Server
export { ServerSqs } from "./server/sqs.server";
// Testing utilities
export { MockClientSqs, type SentMessage } from "./testing/mock-sqs-client";
export {
	MockServerSqs,
	type MockSqsServerOptions,
} from "./testing/mock-sqs-server";
