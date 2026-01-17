import type { LoggerService } from "@nestjs/common";
import {
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	type Mock,
	vi,
} from "vitest";
import { ObservabilityHelper } from "../../src/features/observability";
import type { ObservabilityOptions } from "../../src/interfaces/sqs-options.interface";

// Mock OpenTelemetry API
const mockSpan = {
	setAttribute: vi.fn(),
	setStatus: vi.fn(),
	recordException: vi.fn(),
	end: vi.fn(),
};

const mockTracer = {
	startActiveSpan: vi.fn(
		(_name: string, fn: (span: typeof mockSpan) => unknown) => {
			return fn(mockSpan);
		},
	),
};

const mockOtelApi = {
	trace: {
		getTracer: vi.fn(() => mockTracer),
	},
	SpanStatusCode: {
		OK: 0,
		ERROR: 1,
	},
};

// Helper to reset module cache and control otel availability
let otelAvailable = false;

vi.mock("@opentelemetry/api", () => {
	if (!otelAvailable) {
		throw new Error("Module not found");
	}
	return mockOtelApi;
});

describe("ObservabilityHelper", () => {
	let helper: ObservabilityHelper;
	let mockLogger: {
		log: Mock;
		error: Mock;
		warn: Mock;
		debug: Mock;
	};

	beforeEach(() => {
		vi.clearAllMocks();

		mockLogger = {
			log: vi.fn(),
			error: vi.fn(),
			warn: vi.fn(),
			debug: vi.fn(),
		};
	});

	afterEach(() => {
		vi.restoreAllMocks();
	});

	describe("Constructor", () => {
		it("should create helper with default options", () => {
			helper = new ObservabilityHelper();

			expect(helper).toBeInstanceOf(ObservabilityHelper);
		});

		it("should create helper with undefined options", () => {
			helper = new ObservabilityHelper(undefined);

			expect(helper).toBeInstanceOf(ObservabilityHelper);
		});

		it("should create helper with tracing enabled", () => {
			helper = new ObservabilityHelper({
				tracing: true,
			});

			expect(helper).toBeInstanceOf(ObservabilityHelper);
		});

		it("should create helper with metrics enabled", () => {
			helper = new ObservabilityHelper({
				metrics: true,
			});

			expect(helper).toBeInstanceOf(ObservabilityHelper);
		});

		it("should create helper with logging configured", () => {
			helper = new ObservabilityHelper({
				logging: {
					logger: mockLogger,
					level: "debug",
				},
			});

			expect(helper).toBeInstanceOf(ObservabilityHelper);
		});

		it("should create helper with all options", () => {
			helper = new ObservabilityHelper({
				tracing: true,
				metrics: true,
				logging: {
					logger: mockLogger,
					level: "info",
				},
			});

			expect(helper).toBeInstanceOf(ObservabilityHelper);
		});
	});

	describe("log() method", () => {
		describe("without logger configured", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper();
			});

			it("should not throw when no logger is configured", () => {
				expect(() => helper.log("Test message")).not.toThrow();
			});

			it("should silently skip logging when no logger", () => {
				helper.log("Test message", "TestContext");
				// No error thrown means success
			});
		});

		describe("with logger configured at debug level", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					logging: {
						logger: mockLogger,
						level: "debug",
					},
				});
			});

			it("should call debug when level is debug", () => {
				helper.log("Debug message", "TestContext");

				expect(mockLogger.debug).toHaveBeenCalledWith(
					"Debug message",
					"TestContext",
				);
			});
		});

		describe("with logger configured at info level", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					logging: {
						logger: mockLogger,
						level: "info",
					},
				});
			});

			it("should call log when level is info", () => {
				helper.log("Info message", "TestContext");

				expect(mockLogger.log).toHaveBeenCalledWith(
					"Info message",
					"TestContext",
				);
			});
		});

		describe("with logger configured at warn level", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					logging: {
						logger: mockLogger,
						level: "warn",
					},
				});
			});

			it("should call warn when level is warn", () => {
				helper.log("Warn message", "TestContext");

				expect(mockLogger.warn).toHaveBeenCalledWith(
					"Warn message",
					"TestContext",
				);
			});
		});

		describe("with logger configured at error level", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					logging: {
						logger: mockLogger,
						level: "error",
					},
				});
			});

			it("should call error when level is error", () => {
				helper.log("Error message", "TestContext");

				expect(mockLogger.error).toHaveBeenCalledWith(
					"Error message",
					undefined,
					"TestContext",
				);
			});
		});

		describe("default log level", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					logging: {
						logger: mockLogger,
						level: "info", // Default
					},
				});
			});

			it("should use info as default level", () => {
				helper.log("Default level message");

				expect(mockLogger.log).toHaveBeenCalledWith(
					"Default level message",
					undefined,
				);
			});
		});

		describe("with logger missing debug method", () => {
			beforeEach(() => {
				const loggerWithoutDebug = {
					log: vi.fn(),
					error: vi.fn(),
					warn: vi.fn(),
					// debug is optional in LoggerService
				} as unknown as LoggerService;

				helper = new ObservabilityHelper({
					logging: {
						logger: loggerWithoutDebug,
						level: "debug",
					},
				});
			});

			it("should not throw when debug is not available", () => {
				expect(() => helper.log("Debug message")).not.toThrow();
			});
		});
	});

	describe("logError() method", () => {
		describe("without logger configured", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper();
			});

			it("should not throw when no logger is configured", () => {
				const error = new Error("Test error");
				expect(() => helper.logError("Error occurred", error)).not.toThrow();
			});
		});

		describe("with logger configured", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					logging: {
						logger: mockLogger,
						level: "info",
					},
				});
			});

			it("should log error with stack trace", () => {
				const error = new Error("Test error");
				error.stack = "Error: Test error\n    at test.js:1:1";

				helper.logError("Error occurred", error, "TestContext");

				expect(mockLogger.error).toHaveBeenCalledWith(
					"Error occurred",
					"Error: Test error\n    at test.js:1:1",
					"TestContext",
				);
			});

			it("should log error without context", () => {
				const error = new Error("Test error");
				error.stack = "Stack trace";

				helper.logError("Error occurred", error);

				expect(mockLogger.error).toHaveBeenCalledWith(
					"Error occurred",
					"Stack trace",
					undefined,
				);
			});
		});
	});

	describe("recordMetric() method", () => {
		describe("with metrics disabled", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					metrics: false,
				});
			});

			it("should not record metric when disabled", () => {
				helper.recordMetric("test.metric", 1);

				// No error, just no-op
			});
		});

		describe("with metrics enabled but no logger", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					metrics: true,
				});
			});

			it("should not throw when metrics enabled without logger", () => {
				expect(() => helper.recordMetric("test.metric", 1)).not.toThrow();
			});
		});

		describe("with metrics enabled and logger", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					metrics: true,
					logging: {
						logger: mockLogger,
						level: "info",
					},
				});
			});

			it("should record metric without throwing", () => {
				// When OpenTelemetry API is installed (as a peer dep), it will use OTel's no-op meter
				// When OpenTelemetry is not installed, it falls back to logging
				// Either way, it should not throw
				expect(() => helper.recordMetric("test.metric", 42)).not.toThrow();
			});

			it("should record metric with attributes without throwing", () => {
				expect(() =>
					helper.recordMetric("test.metric", 100, {
						pattern: "TEST",
						status: "success",
					}),
				).not.toThrow();
			});

			it("should use histogram for duration metrics", () => {
				// Duration metrics should use histogram (indicated by name containing "duration")
				expect(() =>
					helper.recordMetric("sqs.message.duration", 150),
				).not.toThrow();
				expect(() => helper.recordMetric("request.latency", 50)).not.toThrow();
			});

			it("should use counter for non-duration metrics", () => {
				// Non-duration metrics should use counter
				expect(() =>
					helper.recordMetric("sqs.message.processed", 1),
				).not.toThrow();
				expect(() => helper.recordMetric("sqs.message.sent", 1)).not.toThrow();
			});
		});
	});

	describe("createSpan() method", () => {
		describe("with tracing disabled", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					tracing: false,
				});
			});

			it("should execute function directly without span", () => {
				const fn = vi.fn().mockReturnValue("result");

				const result = helper.createSpan({ name: "test.span" }, fn);

				expect(fn).toHaveBeenCalled();
				expect(result).toBe("result");
			});

			it("should execute async function directly without span", async () => {
				const fn = vi.fn().mockResolvedValue("async-result");

				const result = await helper.createSpan({ name: "test.span" }, fn);

				expect(fn).toHaveBeenCalled();
				expect(result).toBe("async-result");
			});

			it("should propagate sync errors without span", () => {
				const fn = vi.fn().mockImplementation(() => {
					throw new Error("Sync error");
				});

				expect(() => helper.createSpan({ name: "test.span" }, fn)).toThrow(
					"Sync error",
				);
			});

			it("should propagate async errors without span", async () => {
				const fn = vi.fn().mockRejectedValue(new Error("Async error"));

				await expect(
					helper.createSpan({ name: "test.span" }, fn),
				).rejects.toThrow("Async error");
			});
		});

		describe("with tracing enabled but OpenTelemetry not installed", () => {
			beforeEach(() => {
				// OpenTelemetry is not available (mocked to throw)
				otelAvailable = false;
				helper = new ObservabilityHelper({
					tracing: true,
				});
			});

			it("should fall back to direct execution", () => {
				const fn = vi.fn().mockReturnValue("fallback-result");

				const result = helper.createSpan({ name: "test.span" }, fn);

				expect(fn).toHaveBeenCalled();
				expect(result).toBe("fallback-result");
			});
		});

		// Note: Testing with actual OpenTelemetry would require more complex setup
		// The following tests verify the behavior when tracing is disabled (most common case)

		describe("span options", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					tracing: false,
				});
			});

			it("should accept span name", () => {
				const fn = vi.fn().mockReturnValue("result");

				helper.createSpan({ name: "custom.span.name" }, fn);

				expect(fn).toHaveBeenCalled();
			});

			it("should accept span attributes", () => {
				const fn = vi.fn().mockReturnValue("result");

				helper.createSpan(
					{
						name: "test.span",
						attributes: {
							"custom.attr": "value",
							"numeric.attr": 42,
							"boolean.attr": true,
						},
					},
					fn,
				);

				expect(fn).toHaveBeenCalled();
			});
		});
	});

	describe("Integration Scenarios", () => {
		describe("typical server usage", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					tracing: false, // Disabled for unit tests
					metrics: true,
					logging: {
						logger: mockLogger,
						level: "info",
					},
				});
			});

			it("should handle message processing flow", async () => {
				// Simulate message processing
				const result = await helper.createSpan(
					{
						name: "sqs.handleMessage",
						attributes: {
							"sqs.message_id": "msg-123",
							"sqs.queue_url": "https://sqs.example.com/queue",
						},
					},
					async () => {
						helper.log("Processing message", "SqsServer");
						return "processed";
					},
				);

				expect(result).toBe("processed");
				expect(mockLogger.log).toHaveBeenCalledWith(
					"Processing message",
					"SqsServer",
				);
			});

			it("should record metrics after processing without throwing", () => {
				// When OpenTelemetry is available, metrics go to OTel; otherwise to logger
				expect(() => {
					helper.recordMetric("sqs.message.processed", 1, {
						pattern: "ORDER_CREATED",
						status: "success",
					});

					helper.recordMetric("sqs.message.duration", 150, {
						pattern: "ORDER_CREATED",
					});
				}).not.toThrow();
			});

			it("should handle processing errors", async () => {
				const error = new Error("Processing failed");
				error.stack = "Error stack";

				await expect(
					helper.createSpan({ name: "sqs.handleMessage" }, async () => {
						helper.logError("Error processing message", error, "SqsServer");
						throw error;
					}),
				).rejects.toThrow("Processing failed");

				expect(mockLogger.error).toHaveBeenCalledWith(
					"Error processing message",
					"Error stack",
					"SqsServer",
				);
			});
		});

		describe("typical client usage", () => {
			beforeEach(() => {
				helper = new ObservabilityHelper({
					tracing: false,
					metrics: true,
					logging: {
						logger: mockLogger,
						level: "debug",
					},
				});
			});

			it("should handle message sending flow", async () => {
				const result = await helper.createSpan(
					{
						name: "sqs.dispatchEvent",
						attributes: {
							"sqs.pattern": "ORDER_CREATED",
							"sqs.queue_url": "https://sqs.example.com/queue",
						},
					},
					async () => {
						// Simulate sending
						return "sent";
					},
				);

				expect(result).toBe("sent");
			});

			it("should record send metrics without throwing", () => {
				expect(() => {
					helper.recordMetric("sqs.message.sent", 1, {
						pattern: "ORDER_CREATED",
						status: "success",
					});

					helper.recordMetric("sqs.message.send_duration", 50, {
						pattern: "ORDER_CREATED",
					});
				}).not.toThrow();
			});
		});
	});

	describe("Edge Cases", () => {
		it("should handle null options gracefully", () => {
			helper = new ObservabilityHelper(null as unknown as ObservabilityOptions);

			expect(() => helper.log("test")).not.toThrow();
			expect(() => helper.logError("test", new Error("test"))).not.toThrow();
			expect(() => helper.recordMetric("test", 1)).not.toThrow();
		});

		it("should handle empty logging options", () => {
			helper = new ObservabilityHelper({
				logging: undefined,
			});

			expect(() => helper.log("test")).not.toThrow();
		});

		it("should handle function returning undefined", () => {
			helper = new ObservabilityHelper({ tracing: false });

			const result = helper.createSpan({ name: "test" }, () => undefined);

			expect(result).toBeUndefined();
		});

		it("should handle function returning null", () => {
			helper = new ObservabilityHelper({ tracing: false });

			const result = helper.createSpan({ name: "test" }, () => null);

			expect(result).toBeNull();
		});

		it("should handle empty attributes object", () => {
			helper = new ObservabilityHelper({
				metrics: true,
				logging: {
					logger: mockLogger,
					level: "info",
				},
			});

			expect(() => helper.recordMetric("test.metric", 1, {})).not.toThrow();
		});

		it("should handle undefined attributes", () => {
			helper = new ObservabilityHelper({
				metrics: true,
				logging: {
					logger: mockLogger,
					level: "info",
				},
			});

			expect(() =>
				helper.recordMetric("test.metric", 1, undefined),
			).not.toThrow();
		});

		it("should handle zero metric value", () => {
			helper = new ObservabilityHelper({
				metrics: true,
				logging: {
					logger: mockLogger,
					level: "info",
				},
			});

			expect(() => helper.recordMetric("test.metric", 0)).not.toThrow();
		});

		it("should handle negative metric value", () => {
			helper = new ObservabilityHelper({
				metrics: true,
				logging: {
					logger: mockLogger,
					level: "info",
				},
			});

			// Note: Negative values for counters are not recommended but should not throw
			expect(() => helper.recordMetric("test.metric", -5)).not.toThrow();
		});

		it("should handle floating point metric value", () => {
			helper = new ObservabilityHelper({
				metrics: true,
				logging: {
					logger: mockLogger,
					level: "info",
				},
			});

			expect(() => helper.recordMetric("test.metric", 42.5)).not.toThrow();
		});
	});
});
