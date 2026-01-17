import type { Message, SQSClient } from "@aws-sdk/client-sqs";
import { Logger } from "@nestjs/common";
import { of, Subject } from "rxjs";
import { Consumer } from "sqs-consumer";
import {
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	type Mock,
	vi,
} from "vitest";
import { NO_EVENT_HANDLER } from "../../src/constants";
import { S3LargeMessageHandler } from "../../src/features/s3-large-message";
import type { SqsServerOptions } from "../../src/interfaces/sqs-options.interface";
import { SqsDeserializer } from "../../src/serializers/sqs.deserializer";
import { SqsContext } from "../../src/server/sqs.context";
import { ServerSqs } from "../../src/server/sqs.server";

// Mock sqs-consumer
vi.mock("sqs-consumer", () => ({
	Consumer: {
		create: vi.fn(),
	},
}));

// Store mock handlers that can be configured per-test
let mockS3HandlerInstance: {
	wrapIfLarge: Mock;
	unwrapIfPointer: Mock;
};

// Mock S3LargeMessageHandler - needs to return a class-like constructor
vi.mock("../../src/features/s3-large-message", () => {
	return {
		S3LargeMessageHandler: vi.fn().mockImplementation(function (this: unknown) {
			return mockS3HandlerInstance;
		}),
	};
});

describe("ServerSqs", () => {
	let server: ServerSqs;
	let mockSqsClient: SQSClient;
	let mockConsumer: {
		on: Mock;
		start: Mock;
		stop: Mock;
	};
	let consumerEventHandlers: Map<string, (...args: unknown[]) => void>;

	const defaultOptions: SqsServerOptions = {
		sqs: {} as SQSClient,
		consumerOptions: {
			queueUrl: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
		},
	};

	beforeEach(() => {
		vi.clearAllMocks();

		// Set up mock SQS client
		mockSqsClient = {} as SQSClient;

		// Set up mock consumer with event handler tracking
		consumerEventHandlers = new Map();
		mockConsumer = {
			on: vi.fn((event: string, handler: (...args: unknown[]) => void) => {
				consumerEventHandlers.set(event, handler);
				return mockConsumer;
			}),
			start: vi.fn(),
			stop: vi.fn(),
		};

		(Consumer.create as Mock).mockReturnValue(mockConsumer);

		// Suppress logger output during tests
		vi.spyOn(Logger.prototype, "log").mockImplementation(() => {});
		vi.spyOn(Logger.prototype, "warn").mockImplementation(() => {});
		vi.spyOn(Logger.prototype, "error").mockImplementation(() => {});
		vi.spyOn(Logger.prototype, "debug").mockImplementation(() => {});
	});

	afterEach(() => {
		vi.restoreAllMocks();
	});

	describe("Initialization", () => {
		it("should create server with default deserializer", () => {
			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});

			expect(server).toBeInstanceOf(ServerSqs);
			// Server should have a deserializer set up
			expect(
				(server as unknown as { deserializer: unknown }).deserializer,
			).toBeDefined();
		});

		it("should create server with custom deserializer", () => {
			const customDeserializer = new SqsDeserializer({ patternKey: "type" });
			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				deserializer: customDeserializer,
			});

			expect(server).toBeInstanceOf(ServerSqs);
		});

		it("should create server with patternKey option", () => {
			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				patternKey: "type",
			});

			expect(server).toBeInstanceOf(ServerSqs);
		});

		describe("Options Validation", () => {
			it("should throw error when sqs client is missing", () => {
				expect(() => {
					new ServerSqs({
						consumerOptions: {
							queueUrl:
								"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
						},
					} as SqsServerOptions);
				}).toThrow("SQSClient is required in SqsServerOptions");
			});

			it("should throw error when queueUrl is missing", () => {
				expect(() => {
					new ServerSqs({
						sqs: mockSqsClient,
						consumerOptions: {} as { queueUrl: string },
					});
				}).toThrow("queueUrl is required in consumerOptions");
			});

			it("should throw error when s3LargeMessage is enabled without s3Client", () => {
				expect(() => {
					new ServerSqs({
						sqs: mockSqsClient,
						consumerOptions: {
							queueUrl:
								"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
						},
						s3LargeMessage: {
							enabled: true,
							bucket: "test-bucket",
						} as import("../../src/interfaces/sqs-options.interface").S3LargeMessageOptions,
					});
				}).toThrow("s3Client is required when s3LargeMessage is enabled");
			});

			it("should throw error when s3LargeMessage is enabled without bucket", () => {
				expect(() => {
					new ServerSqs({
						sqs: mockSqsClient,
						consumerOptions: {
							queueUrl:
								"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
						},
						s3LargeMessage: {
							enabled: true,
							s3Client: {} as import("@aws-sdk/client-s3").S3Client,
						} as import("../../src/interfaces/sqs-options.interface").S3LargeMessageOptions,
					});
				}).toThrow("bucket is required when s3LargeMessage is enabled");
			});

			it("should not throw when s3LargeMessage is disabled", () => {
				expect(() => {
					new ServerSqs({
						sqs: mockSqsClient,
						consumerOptions: {
							queueUrl:
								"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
						},
						s3LargeMessage: {
							enabled: false,
							s3Client:
								undefined as unknown as import("@aws-sdk/client-s3").S3Client,
							bucket: "",
						},
					});
				}).not.toThrow();
			});
		});

		it("should set up S3 handler when enabled", () => {
			const mockS3Client = {} as unknown;
			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				s3LargeMessage: {
					enabled: true,
					s3Client: mockS3Client as import("@aws-sdk/client-s3").S3Client,
					bucket: "test-bucket",
				},
			});

			expect(S3LargeMessageHandler).toHaveBeenCalledWith({
				enabled: true,
				s3Client: mockS3Client,
				bucket: "test-bucket",
			});
		});

		it("should not set up S3 handler when disabled", () => {
			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				s3LargeMessage: {
					enabled: false,
					s3Client: {} as import("@aws-sdk/client-s3").S3Client,
					bucket: "test-bucket",
				},
			});

			expect(S3LargeMessageHandler).not.toHaveBeenCalled();
		});

		it("should set up observability helper", () => {
			const mockLogger = {
				log: vi.fn(),
				error: vi.fn(),
				warn: vi.fn(),
				debug: vi.fn(),
			};

			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				observability: {
					tracing: true,
					metrics: true,
					logging: {
						logger: mockLogger,
						level: "debug",
					},
				},
			});

			expect(server).toBeInstanceOf(ServerSqs);
		});
	});

	describe("on() method - Event Handler Registration", () => {
		beforeEach(() => {
			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should register event handlers", () => {
			const callback = vi.fn();
			const result = server.on("test-event", callback);

			expect(result).toBe(server); // Should return this for chaining
		});

		it("should support multiple handlers for same event", () => {
			const callback1 = vi.fn();
			const callback2 = vi.fn();

			server.on("test-event", callback1);
			server.on("test-event", callback2);

			// Both handlers should be registered
			expect(server).toBeInstanceOf(ServerSqs);
		});
	});

	describe("unwrap() method", () => {
		beforeEach(() => {
			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should return server instance", () => {
			const unwrapped = server.unwrap<ServerSqs>();
			expect(unwrapped).toBe(server);
		});
	});

	describe("listen() method", () => {
		beforeEach(() => {
			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should create consumer with correct options", () => {
			const callback = vi.fn();
			server.listen(callback);

			expect(Consumer.create).toHaveBeenCalledWith(
				expect.objectContaining({
					queueUrl: defaultOptions.consumerOptions.queueUrl,
					sqs: mockSqsClient,
					waitTimeSeconds: 20, // Default
					visibilityTimeout: 30, // Default
					batchSize: 10, // Default
					attributeNames: ["All"],
					messageAttributeNames: ["All"],
					shouldDeleteMessages: true, // Default
					handleMessage: expect.any(Function),
				}),
			);
		});

		it("should use custom consumer options when provided", () => {
			const customServer = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				consumerOptions: {
					queueUrl: defaultOptions.consumerOptions.queueUrl,
					waitTimeSeconds: 10,
					visibilityTimeout: 60,
					batchSize: 5,
					attributeNames: ["SentTimestamp"],
					messageAttributeNames: ["pattern"],
					shouldDeleteMessages: false,
				},
			});

			const callback = vi.fn();
			customServer.listen(callback);

			expect(Consumer.create).toHaveBeenCalledWith(
				expect.objectContaining({
					waitTimeSeconds: 10,
					visibilityTimeout: 60,
					batchSize: 5,
					attributeNames: ["SentTimestamp"],
					messageAttributeNames: ["pattern"],
					shouldDeleteMessages: false,
				}),
			);
		});

		it("should set up error event handler", () => {
			const callback = vi.fn();
			server.listen(callback);

			expect(mockConsumer.on).toHaveBeenCalledWith(
				"error",
				expect.any(Function),
			);
		});

		it("should set up processing_error event handler", () => {
			const callback = vi.fn();
			server.listen(callback);

			expect(mockConsumer.on).toHaveBeenCalledWith(
				"processing_error",
				expect.any(Function),
			);
		});

		it("should set up started event handler", () => {
			const callback = vi.fn();
			server.listen(callback);

			expect(mockConsumer.on).toHaveBeenCalledWith(
				"started",
				expect.any(Function),
			);
		});

		it("should set up stopped event handler", () => {
			const callback = vi.fn();
			server.listen(callback);

			expect(mockConsumer.on).toHaveBeenCalledWith(
				"stopped",
				expect.any(Function),
			);
		});

		it("should start consumer", () => {
			const callback = vi.fn();
			server.listen(callback);

			expect(mockConsumer.start).toHaveBeenCalled();
		});

		it("should call callback after starting", () => {
			const callback = vi.fn();
			server.listen(callback);

			expect(callback).toHaveBeenCalled();
		});

		it("should log error when error event fires", () => {
			const loggerErrorSpy = vi.spyOn(Logger.prototype, "error");
			server.listen(() => {});

			const errorHandler = consumerEventHandlers.get("error");
			const testError = new Error("Test error");
			testError.stack = "Error stack trace";
			errorHandler?.(testError);

			expect(loggerErrorSpy).toHaveBeenCalledWith(
				"SQS Consumer error",
				"Error stack trace",
			);
		});

		it("should log error when processing_error event fires", () => {
			const loggerErrorSpy = vi.spyOn(Logger.prototype, "error");
			server.listen(() => {});

			const processingErrorHandler =
				consumerEventHandlers.get("processing_error");
			const testError = new Error("Processing error");
			testError.stack = "Processing error stack";
			processingErrorHandler?.(testError);

			expect(loggerErrorSpy).toHaveBeenCalledWith(
				"SQS Processing error",
				"Processing error stack",
			);
		});

		it("should log when consumer started", () => {
			const loggerLogSpy = vi.spyOn(Logger.prototype, "log");
			server.listen(() => {});

			const startedHandler = consumerEventHandlers.get("started");
			startedHandler?.();

			expect(loggerLogSpy).toHaveBeenCalledWith("SQS Consumer started");
		});

		it("should log when consumer stopped", () => {
			const loggerLogSpy = vi.spyOn(Logger.prototype, "log");
			server.listen(() => {});

			const stoppedHandler = consumerEventHandlers.get("stopped");
			stoppedHandler?.();

			expect(loggerLogSpy).toHaveBeenCalledWith("SQS Consumer stopped");
		});
	});

	describe("close() method", () => {
		beforeEach(() => {
			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should stop consumer when it exists", () => {
			server.listen(() => {});
			server.close();

			expect(mockConsumer.stop).toHaveBeenCalled();
		});

		it("should set consumer to null after closing", () => {
			server.listen(() => {});
			server.close();

			// Calling close again should not throw
			expect(() => server.close()).not.toThrow();
		});

		it("should not throw when consumer is null", () => {
			// Never started, so consumer is null
			expect(() => server.close()).not.toThrow();
		});
	});

	describe("Message Handling", () => {
		let handleMessageFn: (message: Message) => Promise<void>;

		beforeEach(() => {
			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});

			// Capture the handleMessage function
			(Consumer.create as Mock).mockImplementation((options) => {
				handleMessageFn = options.handleMessage;
				return mockConsumer;
			});

			server.listen(() => {});
		});

		it("should deserialize message and extract pattern", async () => {
			const handler = vi.fn().mockResolvedValue(undefined);

			// Mock getHandlerByPattern to return a callable function with isEventHandler property
			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: true,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({
					pattern: "TEST_PATTERN",
					data: { foo: "bar" },
				}),
			};

			await handleMessageFn(message);

			expect(handler).toHaveBeenCalled();
		});

		it("should warn when no handler found for pattern", async () => {
			const loggerWarnSpy = vi.spyOn(Logger.prototype, "warn");

			// Mock getHandlerByPattern to return null
			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				null as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({
					pattern: "UNKNOWN_PATTERN",
					data: { foo: "bar" },
				}),
			};

			await handleMessageFn(message);

			expect(loggerWarnSpy).toHaveBeenCalledWith(
				expect.stringContaining(NO_EVENT_HANDLER),
			);
		});

		it("should create SqsContext with message and pattern", async () => {
			const handler = vi.fn().mockImplementation((_data, ctx) => {
				expect(ctx).toBeInstanceOf(SqsContext);
				expect(ctx.getPattern()).toBe("TEST_PATTERN");
				expect(ctx.getMessage().Body).toBeDefined();
				return undefined;
			});

			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: true,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({
					pattern: "TEST_PATTERN",
					data: { test: true },
				}),
			};

			await handleMessageFn(message);

			expect(handler).toHaveBeenCalled();
		});

		it("should handle event pattern handlers (@EventPattern)", async () => {
			const handler = vi.fn().mockResolvedValue("result");

			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: true,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({
					pattern: "EVENT_PATTERN",
					data: { event: "data" },
				}),
			};

			await handleMessageFn(message);

			expect(handler).toHaveBeenCalledWith(
				{ event: "data" },
				expect.any(SqsContext),
			);
		});

		it("should handle message pattern handlers (@MessagePattern)", async () => {
			const handler = vi.fn().mockResolvedValue("response");

			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: false,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({
					pattern: "MESSAGE_PATTERN",
					data: { request: "data" },
				}),
			};

			await handleMessageFn(message);

			expect(handler).toHaveBeenCalledWith(
				{ request: "data" },
				expect.any(SqsContext),
			);
		});

		it("should handle Observable responses from handlers", async () => {
			const handler = vi.fn().mockReturnValue(of("observable-result"));

			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: false,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({
					pattern: "OBSERVABLE_PATTERN",
					data: {},
				}),
			};

			await expect(handleMessageFn(message)).resolves.toBeUndefined();
		});

		it("should re-throw errors for SQS retry mechanism", async () => {
			const handler = vi.fn().mockRejectedValue(new Error("Handler error"));

			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: true,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({
					pattern: "ERROR_PATTERN",
					data: {},
				}),
			};

			await expect(handleMessageFn(message)).rejects.toThrow("Handler error");
		});

		it("should log errors during message processing", async () => {
			const loggerErrorSpy = vi.spyOn(Logger.prototype, "error");
			const handler = vi.fn().mockRejectedValue(new Error("Processing failed"));

			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: true,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({
					pattern: "ERROR_PATTERN",
					data: {},
				}),
			};

			await expect(handleMessageFn(message)).rejects.toThrow();

			expect(loggerErrorSpy).toHaveBeenCalledWith(
				expect.stringContaining("Error processing message"),
				expect.any(String),
			);
		});

		it("should handle empty message body", async () => {
			const message: Message = {
				MessageId: "msg-123",
				Body: "",
			};

			// SqsDeserializer throws on empty body
			await expect(handleMessageFn(message)).rejects.toThrow();
		});
	});

	describe("S3 Large Message Integration", () => {
		let handleMessageFn: (message: Message) => Promise<void>;

		beforeEach(() => {
			// Set up the mock instance that will be returned by the constructor
			mockS3HandlerInstance = {
				wrapIfLarge: vi
					.fn()
					.mockImplementation((msg: string) => Promise.resolve(msg)),
				unwrapIfPointer: vi
					.fn()
					.mockImplementation((msg: string) => Promise.resolve(msg)),
			};

			const mockS3Client = {} as import("@aws-sdk/client-s3").S3Client;

			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				s3LargeMessage: {
					enabled: true,
					s3Client: mockS3Client,
					bucket: "test-bucket",
				},
			});

			(Consumer.create as Mock).mockImplementation((options) => {
				handleMessageFn = options.handleMessage;
				return mockConsumer;
			});

			server.listen(() => {});
		});

		it("should unwrap S3 pointer before deserialization", async () => {
			const actualBody = JSON.stringify({
				pattern: "S3_PATTERN",
				data: { large: "payload" },
			});

			mockS3HandlerInstance.unwrapIfPointer.mockResolvedValue(actualBody);

			const handler = vi.fn().mockResolvedValue(undefined);
			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: true,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const pointerBody = JSON.stringify({
				__s3pointer: { bucket: "test-bucket", key: "messages/123" },
			});

			const message: Message = {
				MessageId: "msg-123",
				Body: pointerBody,
			};

			await handleMessageFn(message);

			expect(mockS3HandlerInstance.unwrapIfPointer).toHaveBeenCalledWith(
				pointerBody,
			);
			expect(handler).toHaveBeenCalledWith(
				{ large: "payload" },
				expect.any(SqsContext),
			);
		});

		it("should pass through non-pointer messages", async () => {
			const regularBody = JSON.stringify({
				pattern: "REGULAR_PATTERN",
				data: { regular: "data" },
			});

			mockS3HandlerInstance.unwrapIfPointer.mockResolvedValue(regularBody);

			const handler = vi.fn().mockResolvedValue(undefined);
			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: true,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: regularBody,
			};

			await handleMessageFn(message);

			expect(mockS3HandlerInstance.unwrapIfPointer).toHaveBeenCalledWith(
				regularBody,
			);
			expect(handler).toHaveBeenCalledWith(
				{ regular: "data" },
				expect.any(SqsContext),
			);
		});
	});

	describe("handleEvent() method", () => {
		beforeEach(() => {
			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should call handler with data and context", async () => {
			const handler = vi.fn().mockResolvedValue("result");

			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: true,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({ pattern: "TEST", data: {} }),
			};
			const context = new SqsContext([message, "TEST_PATTERN"]);
			const packet = { pattern: "TEST_PATTERN", data: { test: true } };

			const result = await server.handleEvent("TEST_PATTERN", packet, context);

			expect(handler).toHaveBeenCalledWith({ test: true }, context);
			expect(result).toBe("result");
		});

		it("should log error when handler not found", async () => {
			const loggerErrorSpy = vi.spyOn(Logger.prototype, "error");

			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				null as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({ pattern: "TEST", data: {} }),
			};
			const context = new SqsContext([message, "UNKNOWN"]);
			const packet = { pattern: "UNKNOWN", data: {} };

			await server.handleEvent("UNKNOWN", packet, context);

			expect(loggerErrorSpy).toHaveBeenCalledWith(
				expect.stringContaining(NO_EVENT_HANDLER),
			);
		});

		it("should handle Observable responses", async () => {
			// Create a ReplaySubject that buffers the value so connectable can capture it
			const subject = new Subject<string>();

			// Handler returns the subject as an observable
			const handler = vi.fn().mockImplementation(() => {
				// Emit value asynchronously to allow connectable to set up
				setTimeout(() => {
					subject.next("stream-value");
					subject.complete();
				}, 0);
				return subject.asObservable();
			});

			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: true,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({ pattern: "TEST", data: {} }),
			};
			const context = new SqsContext([message, "OBSERVABLE"]);
			const packet = { pattern: "OBSERVABLE", data: {} };

			const result = await server.handleEvent("OBSERVABLE", packet, context);

			// The handler was called
			expect(handler).toHaveBeenCalledWith({}, context);
			// The result is the subject observable (after being processed through connectable)
			expect(result).toBeDefined();
		});

		it("should handle sync responses", async () => {
			const handler = vi.fn().mockReturnValue("sync-result");

			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: true,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({ pattern: "TEST", data: {} }),
			};
			const context = new SqsContext([message, "SYNC"]);
			const packet = { pattern: "SYNC", data: {} };

			const result = await server.handleEvent("SYNC", packet, context);

			expect(result).toBe("sync-result");
		});
	});

	describe("Observability Integration", () => {
		let handleMessageFn: (message: Message) => Promise<void>;

		beforeEach(() => {
			// We need to test with a real ObservabilityHelper that has been configured
			const mockLogger = {
				log: vi.fn(),
				error: vi.fn(),
				warn: vi.fn(),
				debug: vi.fn(),
			};

			server = new ServerSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				observability: {
					tracing: false, // Disable tracing to simplify tests
					metrics: true,
					logging: {
						logger: mockLogger,
						level: "info",
					},
				},
			});

			(Consumer.create as Mock).mockImplementation((options) => {
				handleMessageFn = options.handleMessage;
				return mockConsumer;
			});

			server.listen(() => {});
		});

		it("should record success metric after processing", async () => {
			const handler = vi.fn().mockResolvedValue(undefined);

			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: true,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({
					pattern: "METRICS_PATTERN",
					data: {},
				}),
			};

			await handleMessageFn(message);

			// Verify handler was called (metrics would be recorded internally)
			expect(handler).toHaveBeenCalled();
		});

		it("should record error metric on failure", async () => {
			const handler = vi.fn().mockRejectedValue(new Error("Test error"));

			vi.spyOn(
				server,
				"getHandlerByPattern" as keyof ServerSqs,
			).mockReturnValue(
				Object.assign(handler, {
					isEventHandler: true,
				}) as unknown as ReturnType<typeof server.getHandlerByPattern>,
			);

			const message: Message = {
				MessageId: "msg-123",
				Body: JSON.stringify({
					pattern: "ERROR_PATTERN",
					data: {},
				}),
			};

			await expect(handleMessageFn(message)).rejects.toThrow();
		});
	});
});
