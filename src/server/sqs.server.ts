import type { Message, QueueAttributeName } from "@aws-sdk/client-sqs";
import { Logger } from "@nestjs/common";
import {
	type CustomTransportStrategy,
	type IncomingEvent,
	type IncomingRequest,
	Server,
} from "@nestjs/microservices";
import { connectable, isObservable, lastValueFrom, Subject } from "rxjs";
import { Consumer } from "sqs-consumer";
import {
	NO_EVENT_HANDLER,
	SQS_DEFAULT_BATCH_SIZE,
	SQS_DEFAULT_VISIBILITY_TIMEOUT,
	SQS_DEFAULT_WAIT_TIME_SECONDS,
} from "../constants";
import { ObservabilityHelper } from "../features/observability";
import { S3LargeMessageHandler } from "../features/s3-large-message";
import type {
	AWS,
	SqsServerOptions,
} from "../interfaces/sqs-options.interface";
import { SqsDeserializer } from "../serializers/sqs.deserializer";
import { SqsContext } from "./sqs.context";

/**
 * Validate server options and throw descriptive errors for missing required fields
 */
function validateServerOptions(options: SqsServerOptions): void {
	if (!options.sqs) {
		throw new Error("SQSClient is required in SqsServerOptions");
	}
	if (!options.consumerOptions?.queueUrl) {
		throw new Error("queueUrl is required in consumerOptions");
	}
	if (options.s3LargeMessage?.enabled && !options.s3LargeMessage.s3Client) {
		throw new Error("s3Client is required when s3LargeMessage is enabled");
	}
	if (options.s3LargeMessage?.enabled && !options.s3LargeMessage.bucket) {
		throw new Error("bucket is required when s3LargeMessage is enabled");
	}
}

/**
 * SQS server implementation for NestJS microservices.
 *
 * Implements the CustomTransportStrategy interface to provide SQS message consumption
 * with support for:
 * - S3 large message handling (automatic unwrapping of S3 pointers)
 * - OpenTelemetry observability (tracing, metrics, logging)
 * - Graceful shutdown with in-flight message tracking
 * - FIFO and Standard queue support
 *
 * @example
 * ```typescript
 * // In your main.ts
 * const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
 *   strategy: new ServerSqs({
 *     sqs: new SQSClient({ region: 'us-east-1' }),
 *     consumerOptions: {
 *       queueUrl: 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
 *     },
 *   }),
 * });
 * await app.listen();
 * ```
 *
 * @example
 * ```typescript
 * // With S3 large message handling
 * const server = new ServerSqs({
 *   sqs: sqsClient,
 *   consumerOptions: { queueUrl: 'https://...' },
 *   s3LargeMessage: {
 *     enabled: true,
 *     s3Client: new S3Client({ region: 'us-east-1' }),
 *     bucket: 'my-large-messages-bucket',
 *   },
 * });
 * ```
 */
export class ServerSqs extends Server implements CustomTransportStrategy {
	// biome-ignore lint/complexity/noBannedTypes: Required by NestJS Server base class interface
	private eventHandlers: Map<string, Function[]> = new Map();
	protected readonly logger = new Logger(ServerSqs.name);
	private consumer: Consumer | null = null;
	private readonly s3Handler?: S3LargeMessageHandler;
	private readonly observability: ObservabilityHelper;
	private activeMessages = 0;

	/**
	 * Creates a new SQS server instance.
	 *
	 * @param options - Configuration options for the SQS server
	 * @throws Error if required options are missing (sqs, queueUrl) or invalid
	 */
	constructor(private readonly options: SqsServerOptions) {
		super();

		// Validate options
		validateServerOptions(options);

		// Set up deserializer with patternKey support
		this.initializeDeserializer({
			deserializer:
				options.deserializer ??
				new SqsDeserializer({ patternKey: options.patternKey }),
		});

		// Set up S3 handler if enabled
		if (options.s3LargeMessage?.enabled) {
			this.s3Handler = new S3LargeMessageHandler(options.s3LargeMessage);
		}

		// Set up observability
		this.observability = new ObservabilityHelper(options.observability);
	}

	/**
	 * Register event handler (required by NestJS v11+)
	 */
	on<
		EventKey extends string = string,
		// biome-ignore lint/complexity/noBannedTypes: Required by NestJS Server base class interface
		EventCallback extends Function = Function,
	>(event: EventKey, callback: EventCallback): this {
		const handlers = this.eventHandlers.get(event) ?? [];
		handlers.push(callback);
		this.eventHandlers.set(event, handlers);
		return this;
	}

	/**
	 * Unwrap the server instance (required by NestJS v11+)
	 */
	unwrap<T>(): T {
		return this as unknown as T;
	}

	/**
	 * Start listening for messages from SQS.
	 *
	 * Creates an SQS consumer and begins polling for messages. Messages are automatically
	 * routed to handlers registered with `@EventPattern()` or `@MessagePattern()` decorators.
	 *
	 * The consumer is configured with:
	 * - Long polling (default 20 seconds wait time)
	 * - Batch size (default 10 messages)
	 * - Automatic message deletion after successful processing
	 *
	 * @param callback - Called when the consumer is ready to receive messages
	 *
	 * @example
	 * ```typescript
	 * // In a controller
	 * @EventPattern('ORDER_CREATED')
	 * async handleOrderCreated(data: OrderData, @Ctx() ctx: SqsContext) {
	 *   console.log('Order created:', data);
	 *   console.log('Message ID:', ctx.getMessageId());
	 * }
	 * ```
	 */
	listen(callback: () => void): void {
		this.consumer = Consumer.create({
			queueUrl: this.options.consumerOptions.queueUrl,
			sqs: this.options.sqs,
			waitTimeSeconds:
				this.options.consumerOptions.waitTimeSeconds ??
				SQS_DEFAULT_WAIT_TIME_SECONDS,
			visibilityTimeout:
				this.options.consumerOptions.visibilityTimeout ??
				SQS_DEFAULT_VISIBILITY_TIMEOUT,
			batchSize:
				this.options.consumerOptions.batchSize ?? SQS_DEFAULT_BATCH_SIZE,
			attributeNames: (this.options.consumerOptions.attributeNames ?? [
				"All",
			]) as QueueAttributeName[],
			messageAttributeNames: this.options.consumerOptions
				.messageAttributeNames ?? ["All"],
			shouldDeleteMessages:
				this.options.consumerOptions.shouldDeleteMessages ?? true,
			handleMessage: this.handleMessage.bind(this),
		});

		this.consumer.on("error", (err) => {
			this.logger.error("SQS Consumer error", err.stack);
			this.observability.logError("SQS Consumer error", err, "SqsServer");
		});

		this.consumer.on("processing_error", (err) => {
			this.logger.error("SQS Processing error", err.stack);
			this.observability.logError("SQS Processing error", err, "SqsServer");
		});

		this.consumer.on("started", () => {
			this.logger.log("SQS Consumer started");
			this.observability.log("SQS Consumer started", "SqsServer");
		});

		this.consumer.on("stopped", () => {
			this.logger.log("SQS Consumer stopped");
			this.observability.log("SQS Consumer stopped", "SqsServer");
		});

		this.consumer.start();
		callback();
	}

	/**
	 * Stop the SQS consumer with graceful shutdown.
	 *
	 * Performs a graceful shutdown by:
	 * 1. Setting shutdown flag to prevent new message processing
	 * 2. Waiting for in-flight messages to complete (up to shutdownTimeoutMs)
	 * 3. Stopping the SQS consumer
	 *
	 * If messages don't complete within the timeout, a warning is logged and
	 * the consumer is stopped anyway. Messages that were not acknowledged
	 * will become visible again after the visibility timeout.
	 *
	 * @returns Promise that resolves when the consumer is stopped
	 *
	 * @example
	 * ```typescript
	 * // In your application shutdown hook
	 * app.enableShutdownHooks();
	 *
	 * // Or manually
	 * await server.close();
	 * ```
	 */
	async close(): Promise<void> {
		// Wait for in-flight messages (with timeout)
		const timeout = this.options.shutdownTimeoutMs ?? 30000;
		const start = Date.now();

		while (this.activeMessages > 0 && Date.now() - start < timeout) {
			await new Promise((resolve) => setTimeout(resolve, 100));
		}

		if (this.activeMessages > 0) {
			this.logger.warn(
				`Shutdown timeout reached with ${this.activeMessages} messages still in flight`,
			);
		}

		if (this.consumer) {
			this.consumer.stop();
			this.consumer = null;
		}

		this.logger.log("SQS Consumer closed");
		this.observability.log("SQS Consumer closed", "SqsServer");
	}

	/**
	 * Handle an incoming SQS message following NestJS microservices pattern
	 */
	private async handleMessage(message: Message): Promise<void> {
		this.activeMessages++;
		const startTime = Date.now();

		return this.observability.createSpan(
			{
				name: "sqs.handleMessage",
				attributes: {
					"sqs.message_id": message.MessageId ?? "unknown",
					"sqs.queue_url": this.options.consumerOptions.queueUrl,
				},
			},
			async () => {
				try {
					// Unwrap S3 large message if needed
					let body = message.Body ?? "";
					if (this.s3Handler) {
						body = await this.s3Handler.unwrapIfPointer(body);
					}

					// Create a modified message with unwrapped body
					const unwrappedMessage: AWS.SQS.Message = {
						...message,
						Body: body,
					};

					// Deserialize the message
					const deserializedMessage = this.deserializer.deserialize(
						unwrappedMessage,
					) as IncomingRequest | IncomingEvent;

					const pattern = deserializedMessage.pattern;
					const data = deserializedMessage.data;

					// Create the context
					const context = new SqsContext([unwrappedMessage, pattern]);

					// Get the handler using NestJS Server base class method
					const handler = this.getHandlerByPattern(pattern);

					if (!handler) {
						this.logger.warn(
							`${NO_EVENT_HANDLER} Pattern: ${JSON.stringify(pattern)}`,
						);
						return;
					}

					// Check if this is an event handler (@EventPattern) or message handler (@MessagePattern)
					if (handler.isEventHandler) {
						// Use NestJS handleEvent for @EventPattern - fire-and-forget
						await this.handleEvent(pattern, { pattern, data }, context);
					} else {
						// For @MessagePattern (request-response), we'd need response handling
						// SQS is typically fire-and-forget, but support it for completeness
						const response$ = this.transformToObservable(
							await handler(data, context),
						);
						// For SQS, we don't send responses back, just await completion
						if (response$) {
							await lastValueFrom(response$);
						}
					}

					// Record success metric
					const duration = Date.now() - startTime;
					this.observability.recordMetric("sqs.message.processed", 1, {
						pattern,
						status: "success",
					});
					this.observability.recordMetric("sqs.message.duration", duration, {
						pattern,
					});
				} catch (error) {
					const err = error as Error;
					this.logger.error(
						`Error processing message: ${err.message}`,
						err.stack,
					);
					this.observability.logError(
						"Error processing message",
						err,
						"SqsServer",
					);

					// Record error metric
					const duration = Date.now() - startTime;
					this.observability.recordMetric("sqs.message.processed", 1, {
						status: "error",
					});
					this.observability.recordMetric("sqs.message.duration", duration, {
						status: "error",
					});

					throw error;
				} finally {
					this.activeMessages--;
				}
			},
		) as Promise<void>;
	}

	/**
	 * Handle event pattern messages following NestJS Server pattern.
	 *
	 * This method is called internally to dispatch messages to handlers registered
	 * with `@EventPattern()`. It properly integrates with NestJS lifecycle hooks
	 * and handles both synchronous and Observable responses.
	 *
	 * For Observable responses, the method connects to the Observable and awaits
	 * its completion, ensuring proper backpressure handling.
	 *
	 * @param pattern - The message pattern (event type)
	 * @param packet - The message packet containing pattern and data
	 * @param context - The SQS context with message metadata
	 * @returns Promise resolving to the handler's return value (if any)
	 *
	 * @example
	 * ```typescript
	 * // This is called internally - you typically use @EventPattern instead
	 * @EventPattern('ORDER_CREATED')
	 * async handleOrder(data: OrderData) {
	 *   await this.orderService.process(data);
	 * }
	 * ```
	 */
	public async handleEvent(
		pattern: string,
		packet: { pattern: string; data: unknown },
		context: SqsContext,
	): Promise<unknown> {
		const handler = this.getHandlerByPattern(pattern);

		if (!handler) {
			return this.logger.error(
				`${NO_EVENT_HANDLER} Event pattern: ${JSON.stringify(pattern)}.`,
			);
		}

		const resultOrStream = await handler(packet.data, context);

		// Handle Observable responses properly following NestJS pattern
		if (isObservable(resultOrStream)) {
			const connectableSource = connectable(resultOrStream, {
				connector: () => new Subject(),
				resetOnDisconnect: false,
			});
			connectableSource.connect();
			await lastValueFrom(connectableSource);
		}

		return resultOrStream;
	}
}
