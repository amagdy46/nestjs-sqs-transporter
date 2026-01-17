import type { SQSClient } from "@aws-sdk/client-sqs";
import { Logger } from "@nestjs/common";
import { Producer } from "sqs-producer";
import {
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	type Mock,
	vi,
} from "vitest";
import { ClientSqs } from "../../src/client/sqs.client";
import {
	CORRELATION_ID_HEADER,
	MESSAGE_PATTERN_HEADER,
} from "../../src/constants";
import { S3LargeMessageHandler } from "../../src/features/s3-large-message";
import type { SqsClientOptions } from "../../src/interfaces/sqs-options.interface";
import { SqsSerializer } from "../../src/serializers/sqs.serializer";

// Mock sqs-producer
vi.mock("sqs-producer", () => ({
	Producer: {
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

// Mock uuid
vi.mock("uuid", () => ({
	v4: vi.fn(() => "mock-uuid-123"),
}));

describe("ClientSqs", () => {
	let client: ClientSqs;
	let mockSqsClient: SQSClient;
	let mockProducer: {
		send: Mock;
	};

	const defaultOptions: SqsClientOptions = {
		sqs: {} as SQSClient,
		queueUrl: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
	};

	beforeEach(() => {
		vi.clearAllMocks();

		// Set up mock SQS client
		mockSqsClient = {} as SQSClient;

		// Set up mock producer
		mockProducer = {
			send: vi.fn().mockResolvedValue([{ MessageId: "sent-msg-123" }]),
		};

		(Producer.create as Mock).mockReturnValue(mockProducer);

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
		it("should create client with default options", () => {
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});

			expect(client).toBeInstanceOf(ClientSqs);
			expect(Producer.create).toHaveBeenCalledWith({
				queueUrl: defaultOptions.queueUrl,
				sqs: mockSqsClient,
			});
		});

		it("should create client with custom serializer", () => {
			const customSerializer = new SqsSerializer({ patternKey: "type" });
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				serializer: customSerializer,
			});

			expect(client).toBeInstanceOf(ClientSqs);
		});

		it("should create client with patternKey option", () => {
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				patternKey: "type",
			});

			expect(client).toBeInstanceOf(ClientSqs);
		});

		describe("Options Validation", () => {
			it("should throw error when sqs client is missing", () => {
				expect(() => {
					new ClientSqs({
						queueUrl:
							"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
					} as SqsClientOptions);
				}).toThrow("SQSClient is required in SqsClientOptions");
			});

			it("should throw error when queueUrl is missing", () => {
				expect(() => {
					new ClientSqs({
						sqs: mockSqsClient,
					} as SqsClientOptions);
				}).toThrow("queueUrl is required in SqsClientOptions");
			});

			it("should throw error when s3LargeMessage is enabled without s3Client", () => {
				expect(() => {
					new ClientSqs({
						sqs: mockSqsClient,
						queueUrl:
							"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
						s3LargeMessage: {
							enabled: true,
							bucket: "test-bucket",
						} as import("../../src/interfaces/sqs-options.interface").S3LargeMessageOptions,
					});
				}).toThrow("s3Client is required when s3LargeMessage is enabled");
			});

			it("should throw error when s3LargeMessage is enabled without bucket", () => {
				expect(() => {
					new ClientSqs({
						sqs: mockSqsClient,
						queueUrl:
							"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
						s3LargeMessage: {
							enabled: true,
							s3Client: {} as import("@aws-sdk/client-s3").S3Client,
						} as import("../../src/interfaces/sqs-options.interface").S3LargeMessageOptions,
					});
				}).toThrow("bucket is required when s3LargeMessage is enabled");
			});

			it("should not throw when s3LargeMessage is disabled", () => {
				expect(() => {
					new ClientSqs({
						sqs: mockSqsClient,
						queueUrl:
							"https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
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
			client = new ClientSqs({
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
			client = new ClientSqs({
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

			client = new ClientSqs({
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

			expect(client).toBeInstanceOf(ClientSqs);
		});
	});

	describe("connect() method", () => {
		beforeEach(() => {
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should set connected flag to true", async () => {
			await client.connect();

			// The connected flag is private, but we can verify through logs
			expect(Logger.prototype.log).toHaveBeenCalledWith("SQS Client connected");
		});

		it("should log connection", async () => {
			const logSpy = vi.spyOn(Logger.prototype, "log");
			await client.connect();

			expect(logSpy).toHaveBeenCalledWith("SQS Client connected");
		});
	});

	describe("close() method", () => {
		beforeEach(() => {
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should set connected flag to false", () => {
			client.close();

			expect(Logger.prototype.log).toHaveBeenCalledWith("SQS Client closed");
		});

		it("should log disconnection", () => {
			const logSpy = vi.spyOn(Logger.prototype, "log");
			client.close();

			expect(logSpy).toHaveBeenCalledWith("SQS Client closed");
		});
	});

	describe("unwrap() method", () => {
		beforeEach(() => {
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should return client instance", () => {
			const unwrapped = client.unwrap<ClientSqs>();
			expect(unwrapped).toBe(client);
		});
	});

	describe("publish() method (request-response pattern)", () => {
		beforeEach(() => {
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should send message and call callback with success response", async () => {
			const result = await new Promise<{ response?: unknown; err?: Error }>(
				(resolve) => {
					const callback = (packet: { response?: unknown; err?: Error }) => {
						resolve(packet);
					};

					// Access protected method via type assertion
					(
						client as unknown as {
							publish: (packet: unknown, callback: unknown) => unknown;
						}
					).publish(
						{ pattern: "TEST_PATTERN", data: { foo: "bar" } },
						callback,
					);
				},
			);

			expect(result.response).toEqual({ success: true });
		});

		it("should serialize message correctly", async () => {
			const callback = vi.fn();

			(
				client as unknown as {
					publish: (packet: unknown, callback: unknown) => unknown;
				}
			).publish({ pattern: "TEST_PATTERN", data: { foo: "bar" } }, callback);

			// Wait for async operation
			await new Promise((resolve) => setTimeout(resolve, 10));

			expect(mockProducer.send).toHaveBeenCalledWith([
				expect.objectContaining({
					id: expect.any(String),
					body: expect.stringContaining("TEST_PATTERN"),
					messageAttributes: expect.objectContaining({
						[MESSAGE_PATTERN_HEADER]: {
							DataType: "String",
							StringValue: "TEST_PATTERN",
						},
					}),
				}),
			]);
		});

		it("should include correlation ID in message attributes", async () => {
			const callback = vi.fn();

			(
				client as unknown as {
					publish: (packet: unknown, callback: unknown) => unknown;
				}
			).publish({ pattern: "TEST_PATTERN", data: {} }, callback);

			await new Promise((resolve) => setTimeout(resolve, 10));

			expect(mockProducer.send).toHaveBeenCalledWith([
				expect.objectContaining({
					messageAttributes: expect.objectContaining({
						[CORRELATION_ID_HEADER]: {
							DataType: "String",
							StringValue: expect.any(String),
						},
					}),
				}),
			]);
		});

		it("should call callback with error on failure", async () => {
			mockProducer.send.mockRejectedValue(new Error("Send failed"));

			const result = await new Promise<{ response?: unknown; err?: Error }>(
				(resolve) => {
					const callback = (packet: { response?: unknown; err?: Error }) => {
						resolve(packet);
					};

					(
						client as unknown as {
							publish: (packet: unknown, callback: unknown) => unknown;
						}
					).publish({ pattern: "TEST_PATTERN", data: {} }, callback);
				},
			);

			expect(result.err).toBeInstanceOf(Error);
			expect(result.err?.message).toBe("Send failed");
		});

		it("should return cleanup function", () => {
			const callback = vi.fn();

			const cleanup = (
				client as unknown as {
					publish: (packet: unknown, callback: unknown) => unknown;
				}
			).publish({ pattern: "TEST_PATTERN", data: {} }, callback);

			expect(typeof cleanup).toBe("function");
			// Cleanup should not throw
			expect(() => (cleanup as () => void)()).not.toThrow();
		});
	});

	describe("dispatchEvent() method (fire-and-forget pattern)", () => {
		beforeEach(() => {
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should send message without waiting for response", async () => {
			// dispatchEvent is protected, access via emit which calls it
			const result = await (
				client as unknown as {
					dispatchEvent: (packet: unknown) => Promise<unknown>;
				}
			).dispatchEvent({ pattern: "EVENT_PATTERN", data: { event: "data" } });

			expect(result).toBeUndefined();
			expect(mockProducer.send).toHaveBeenCalled();
		});

		it("should serialize message correctly for events", async () => {
			await (
				client as unknown as {
					dispatchEvent: (packet: unknown) => Promise<unknown>;
				}
			).dispatchEvent({ pattern: "EVENT_PATTERN", data: { event: "data" } });

			expect(mockProducer.send).toHaveBeenCalledWith([
				expect.objectContaining({
					body: expect.stringContaining("EVENT_PATTERN"),
					messageAttributes: expect.objectContaining({
						[MESSAGE_PATTERN_HEADER]: {
							DataType: "String",
							StringValue: "EVENT_PATTERN",
						},
					}),
				}),
			]);
		});

		it("should not include correlation ID for events", async () => {
			await (
				client as unknown as {
					dispatchEvent: (packet: unknown) => Promise<unknown>;
				}
			).dispatchEvent({ pattern: "EVENT_PATTERN", data: {} });

			const sentMessage = mockProducer.send.mock.calls[0][0][0];
			expect(
				sentMessage.messageAttributes[CORRELATION_ID_HEADER],
			).toBeUndefined();
		});
	});

	describe("S3 Large Message Integration", () => {
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

			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				s3LargeMessage: {
					enabled: true,
					s3Client: mockS3Client,
					bucket: "test-bucket",
				},
			});
		});

		it("should wrap large messages before sending", async () => {
			const largeData = { large: "x".repeat(300000) };
			const s3Pointer = JSON.stringify({
				__s3pointer: { bucket: "test-bucket", key: "messages/123" },
			});

			mockS3HandlerInstance.wrapIfLarge.mockResolvedValue(s3Pointer);

			await (
				client as unknown as {
					dispatchEvent: (packet: unknown) => Promise<unknown>;
				}
			).dispatchEvent({ pattern: "LARGE_EVENT", data: largeData });

			expect(mockS3HandlerInstance.wrapIfLarge).toHaveBeenCalled();
			expect(mockProducer.send).toHaveBeenCalledWith([
				expect.objectContaining({
					body: s3Pointer,
				}),
			]);
		});

		it("should pass through small messages unchanged", async () => {
			const smallData = { small: "data" };
			const serializedBody = JSON.stringify({
				pattern: "SMALL_EVENT",
				data: smallData,
				id: "mock-uuid-123",
			});

			mockS3HandlerInstance.wrapIfLarge.mockResolvedValue(serializedBody);

			await (
				client as unknown as {
					dispatchEvent: (packet: unknown) => Promise<unknown>;
				}
			).dispatchEvent({ pattern: "SMALL_EVENT", data: smallData });

			expect(mockS3HandlerInstance.wrapIfLarge).toHaveBeenCalledWith(
				expect.stringContaining("SMALL_EVENT"),
			);
		});
	});

	describe("FIFO Queue Support", () => {
		describe("with static messageGroupId", () => {
			beforeEach(() => {
				client = new ClientSqs({
					...defaultOptions,
					sqs: mockSqsClient,
					fifo: {
						enabled: true,
						messageGroupId: "static-group",
					},
				});
			});

			it("should include groupId in message", async () => {
				await (
					client as unknown as {
						dispatchEvent: (packet: unknown) => Promise<unknown>;
					}
				).dispatchEvent({ pattern: "FIFO_EVENT", data: {} });

				expect(mockProducer.send).toHaveBeenCalledWith([
					expect.objectContaining({
						groupId: "static-group",
					}),
				]);
			});

			it("should include auto-generated deduplicationId", async () => {
				await (
					client as unknown as {
						dispatchEvent: (packet: unknown) => Promise<unknown>;
					}
				).dispatchEvent({ pattern: "FIFO_EVENT", data: { key: "value" } });

				expect(mockProducer.send).toHaveBeenCalledWith([
					expect.objectContaining({
						deduplicationId: expect.any(String),
					}),
				]);
			});
		});

		describe("with function messageGroupId", () => {
			beforeEach(() => {
				client = new ClientSqs({
					...defaultOptions,
					sqs: mockSqsClient,
					fifo: {
						enabled: true,
						messageGroupId: (pattern, data) =>
							`${pattern}-${(data as { userId?: string }).userId ?? "default"}`,
					},
				});
			});

			it("should call messageGroupId function with pattern and data", async () => {
				await (
					client as unknown as {
						dispatchEvent: (packet: unknown) => Promise<unknown>;
					}
				).dispatchEvent({
					pattern: "USER_EVENT",
					data: { userId: "user-123" },
				});

				expect(mockProducer.send).toHaveBeenCalledWith([
					expect.objectContaining({
						groupId: "USER_EVENT-user-123",
					}),
				]);
			});
		});

		describe("with default messageGroupId (pattern-based)", () => {
			beforeEach(() => {
				client = new ClientSqs({
					...defaultOptions,
					sqs: mockSqsClient,
					fifo: {
						enabled: true,
					},
				});
			});

			it("should use pattern as groupId when not specified", async () => {
				await (
					client as unknown as {
						dispatchEvent: (packet: unknown) => Promise<unknown>;
					}
				).dispatchEvent({ pattern: "ORDER_CREATED", data: {} });

				expect(mockProducer.send).toHaveBeenCalledWith([
					expect.objectContaining({
						groupId: "ORDER_CREATED",
					}),
				]);
			});
		});

		describe("with custom deduplicationId function", () => {
			beforeEach(() => {
				client = new ClientSqs({
					...defaultOptions,
					sqs: mockSqsClient,
					fifo: {
						enabled: true,
						deduplicationId: (_pattern, data) =>
							`custom-${(data as { orderId?: string }).orderId ?? "unknown"}`,
					},
				});
			});

			it("should use custom deduplicationId function", async () => {
				await (
					client as unknown as {
						dispatchEvent: (packet: unknown) => Promise<unknown>;
					}
				).dispatchEvent({
					pattern: "ORDER_EVENT",
					data: { orderId: "order-456" },
				});

				expect(mockProducer.send).toHaveBeenCalledWith([
					expect.objectContaining({
						deduplicationId: "custom-order-456",
					}),
				]);
			});
		});

		describe("with contentBasedDeduplication enabled", () => {
			beforeEach(() => {
				client = new ClientSqs({
					...defaultOptions,
					sqs: mockSqsClient,
					fifo: {
						enabled: true,
						contentBasedDeduplication: true,
					},
				});
			});

			it("should not include deduplicationId when contentBasedDeduplication is true", async () => {
				await (
					client as unknown as {
						dispatchEvent: (packet: unknown) => Promise<unknown>;
					}
				).dispatchEvent({ pattern: "CBD_EVENT", data: {} });

				const sentMessage = mockProducer.send.mock.calls[0][0][0];
				expect(sentMessage.deduplicationId).toBeUndefined();
			});
		});
	});

	describe("Error Handling", () => {
		beforeEach(() => {
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should log error when send fails", async () => {
			const loggerErrorSpy = vi.spyOn(Logger.prototype, "error");
			mockProducer.send.mockRejectedValue(new Error("SQS send error"));

			await expect(
				(
					client as unknown as {
						dispatchEvent: (packet: unknown) => Promise<unknown>;
					}
				).dispatchEvent({ pattern: "ERROR_EVENT", data: {} }),
			).rejects.toThrow("SQS send error");

			expect(loggerErrorSpy).toHaveBeenCalledWith(
				expect.stringContaining("Error sending message to SQS"),
				expect.any(String),
			);
		});

		it("should re-throw errors after logging", async () => {
			mockProducer.send.mockRejectedValue(new Error("Network error"));

			await expect(
				(
					client as unknown as {
						dispatchEvent: (packet: unknown) => Promise<unknown>;
					}
				).dispatchEvent({ pattern: "NETWORK_ERROR_EVENT", data: {} }),
			).rejects.toThrow("Network error");
		});
	});

	describe("Observability Integration", () => {
		let mockLogger: {
			log: Mock;
			error: Mock;
			warn: Mock;
			debug: Mock;
		};

		beforeEach(() => {
			mockLogger = {
				log: vi.fn(),
				error: vi.fn(),
				warn: vi.fn(),
				debug: vi.fn(),
			};

			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				observability: {
					tracing: false,
					metrics: true,
					logging: {
						logger: mockLogger,
						level: "info", // Use 'info' level so log() calls mockLogger.log()
					},
				},
			});
		});

		it("should log successful message send via observability logger", async () => {
			await (
				client as unknown as {
					dispatchEvent: (packet: unknown) => Promise<unknown>;
				}
			).dispatchEvent({ pattern: "OBSERVED_EVENT", data: {} });

			// Debug log should be called (via Logger.prototype.debug, not mockLogger)
			expect(Logger.prototype.debug).toHaveBeenCalledWith(
				expect.stringContaining("Message sent to SQS"),
			);
		});

		it("should call observability on connect", async () => {
			await client.connect();

			// Observability logs through its configured logger at 'info' level
			expect(mockLogger.log).toHaveBeenCalledWith(
				"SQS Client connected",
				"SqsClient",
			);
		});

		it("should call observability on close", () => {
			client.close();

			expect(mockLogger.log).toHaveBeenCalledWith(
				"SQS Client closed",
				"SqsClient",
			);
		});
	});

	describe("Message Serialization", () => {
		beforeEach(() => {
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should serialize complex nested data", async () => {
			const complexData = {
				nested: {
					array: [1, 2, 3],
					object: { deep: { value: true } },
				},
				nullValue: null,
				number: 42,
			};

			await (
				client as unknown as {
					dispatchEvent: (packet: unknown) => Promise<unknown>;
				}
			).dispatchEvent({ pattern: "COMPLEX_EVENT", data: complexData });

			const sentBody = mockProducer.send.mock.calls[0][0][0].body;
			const parsed = JSON.parse(sentBody);

			expect(parsed.data).toEqual(complexData);
		});

		it("should handle null data", async () => {
			await (
				client as unknown as {
					dispatchEvent: (packet: unknown) => Promise<unknown>;
				}
			).dispatchEvent({ pattern: "NULL_EVENT", data: null });

			const sentBody = mockProducer.send.mock.calls[0][0][0].body;
			const parsed = JSON.parse(sentBody);

			expect(parsed.data).toBeNull();
		});

		it("should include message ID in serialized body", async () => {
			await (
				client as unknown as {
					dispatchEvent: (packet: unknown) => Promise<unknown>;
				}
			).dispatchEvent({ pattern: "ID_EVENT", data: {} });

			const sentBody = mockProducer.send.mock.calls[0][0][0].body;
			const parsed = JSON.parse(sentBody);

			expect(parsed.id).toBe("mock-uuid-123");
		});
	});

	describe("Custom patternKey", () => {
		beforeEach(() => {
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				patternKey: "type",
			});
		});

		it("should serialize with custom pattern key", async () => {
			await (
				client as unknown as {
					dispatchEvent: (packet: unknown) => Promise<unknown>;
				}
			).dispatchEvent({ pattern: "CUSTOM_KEY_EVENT", data: { foo: "bar" } });

			const sentBody = mockProducer.send.mock.calls[0][0][0].body;
			const parsed = JSON.parse(sentBody);

			expect(parsed.type).toBe("CUSTOM_KEY_EVENT");
			expect(parsed.pattern).toBeUndefined();
		});
	});

	describe("emitBatch() method", () => {
		beforeEach(() => {
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
			});
		});

		it("should send multiple messages in a single call", async () => {
			const messages = [
				{ pattern: "ORDER_CREATED", data: { orderId: "123" } },
				{ pattern: "ORDER_CREATED", data: { orderId: "456" } },
				{ pattern: "USER_UPDATED", data: { userId: "789" } },
			];

			mockProducer.send.mockResolvedValue([
				{ MessageId: "msg-1" },
				{ MessageId: "msg-2" },
				{ MessageId: "msg-3" },
			]);

			const result = await client.emitBatch(messages);

			expect(mockProducer.send).toHaveBeenCalledTimes(1);
			expect(mockProducer.send).toHaveBeenCalledWith(
				expect.arrayContaining([
					expect.objectContaining({
						body: expect.stringContaining("ORDER_CREATED"),
					}),
					expect.objectContaining({
						body: expect.stringContaining("ORDER_CREATED"),
					}),
					expect.objectContaining({
						body: expect.stringContaining("USER_UPDATED"),
					}),
				]),
			);
			expect(result).toHaveLength(3);
		});

		it("should split batches larger than 10 messages", async () => {
			// Create 15 messages (should result in 2 batches: 10 + 5)
			const messages = Array.from({ length: 15 }, (_, i) => ({
				pattern: "TEST_EVENT",
				data: { index: i },
			}));

			mockProducer.send
				.mockResolvedValueOnce(
					Array.from({ length: 10 }, (_, i) => ({ MessageId: `msg-${i}` })),
				)
				.mockResolvedValueOnce(
					Array.from({ length: 5 }, (_, i) => ({ MessageId: `msg-${i + 10}` })),
				);

			const result = await client.emitBatch(messages);

			expect(mockProducer.send).toHaveBeenCalledTimes(2);
			// First batch should have 10 messages
			expect(mockProducer.send.mock.calls[0][0]).toHaveLength(10);
			// Second batch should have 5 messages
			expect(mockProducer.send.mock.calls[1][0]).toHaveLength(5);
			expect(result).toHaveLength(15);
		});

		it("should handle exactly 10 messages in one batch", async () => {
			const messages = Array.from({ length: 10 }, (_, i) => ({
				pattern: "TEST_EVENT",
				data: { index: i },
			}));

			mockProducer.send.mockResolvedValue(
				Array.from({ length: 10 }, (_, i) => ({ MessageId: `msg-${i}` })),
			);

			const result = await client.emitBatch(messages);

			expect(mockProducer.send).toHaveBeenCalledTimes(1);
			expect(mockProducer.send.mock.calls[0][0]).toHaveLength(10);
			expect(result).toHaveLength(10);
		});

		it("should handle empty batch", async () => {
			const result = await client.emitBatch([]);

			expect(mockProducer.send).not.toHaveBeenCalled();
			expect(result).toHaveLength(0);
		});

		it("should include message pattern header for each message", async () => {
			const messages = [
				{ pattern: "PATTERN_A", data: {} },
				{ pattern: "PATTERN_B", data: {} },
			];

			mockProducer.send.mockResolvedValue([
				{ MessageId: "msg-1" },
				{ MessageId: "msg-2" },
			]);

			await client.emitBatch(messages);

			const sentMessages = mockProducer.send.mock.calls[0][0];
			expect(sentMessages[0].messageAttributes.messagePattern.StringValue).toBe(
				"PATTERN_A",
			);
			expect(sentMessages[1].messageAttributes.messagePattern.StringValue).toBe(
				"PATTERN_B",
			);
		});

		it("should throw error on batch failure", async () => {
			const messages = [{ pattern: "TEST", data: {} }];
			mockProducer.send.mockRejectedValue(new Error("Batch send failed"));

			await expect(client.emitBatch(messages)).rejects.toThrow(
				"Batch send failed",
			);
		});

		it("should log debug message on success", async () => {
			const loggerDebugSpy = vi.spyOn(Logger.prototype, "debug");
			const messages = [{ pattern: "TEST", data: {} }];

			mockProducer.send.mockResolvedValue([{ MessageId: "msg-1" }]);

			await client.emitBatch(messages);

			expect(loggerDebugSpy).toHaveBeenCalledWith(
				expect.stringContaining("Batch of 1 messages sent"),
			);
		});
	});

	describe("emitBatch() with FIFO queue", () => {
		beforeEach(() => {
			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				fifo: {
					enabled: true,
					messageGroupId: "test-group",
				},
			});

			mockProducer.send.mockResolvedValue([{ MessageId: "msg-1" }]);
		});

		it("should include groupId for FIFO messages", async () => {
			const messages = [{ pattern: "FIFO_EVENT", data: {} }];

			await client.emitBatch(messages);

			const sentMessage = mockProducer.send.mock.calls[0][0][0];
			expect(sentMessage.groupId).toBe("test-group");
		});

		it("should include deduplicationId for FIFO messages", async () => {
			const messages = [{ pattern: "FIFO_EVENT", data: { key: "value" } }];

			await client.emitBatch(messages);

			const sentMessage = mockProducer.send.mock.calls[0][0][0];
			expect(sentMessage.deduplicationId).toBeDefined();
		});
	});

	describe("emitBatch() with S3 large messages", () => {
		beforeEach(() => {
			mockS3HandlerInstance = {
				wrapIfLarge: vi
					.fn()
					.mockImplementation((msg: string) => Promise.resolve(msg)),
				unwrapIfPointer: vi
					.fn()
					.mockImplementation((msg: string) => Promise.resolve(msg)),
			};

			client = new ClientSqs({
				...defaultOptions,
				sqs: mockSqsClient,
				s3LargeMessage: {
					enabled: true,
					s3Client: {} as import("@aws-sdk/client-s3").S3Client,
					bucket: "test-bucket",
				},
			});

			mockProducer.send.mockResolvedValue([{ MessageId: "msg-1" }]);
		});

		it("should wrap large messages with S3 pointer", async () => {
			const s3Pointer = JSON.stringify({
				__s3pointer: { bucket: "test", key: "key" },
			});
			mockS3HandlerInstance.wrapIfLarge.mockResolvedValue(s3Pointer);

			const messages = [
				{ pattern: "LARGE_EVENT", data: { large: "x".repeat(300000) } },
			];

			await client.emitBatch(messages);

			expect(mockS3HandlerInstance.wrapIfLarge).toHaveBeenCalled();
			expect(mockProducer.send.mock.calls[0][0][0].body).toBe(s3Pointer);
		});
	});
});
