import type { LoggerService } from "@nestjs/common";
import type { ObservabilityOptions } from "../interfaces/sqs-options.interface";

// Types from OpenTelemetry API (defined here to avoid hard dependency)
interface OtelCounter {
	add(value: number, attributes?: Record<string, string>): void;
}

interface OtelHistogram {
	record(value: number, attributes?: Record<string, string>): void;
}

interface OtelMeter {
	createCounter(
		name: string,
		options?: { unit?: string; description?: string },
	): OtelCounter;
	createHistogram(
		name: string,
		options?: { unit?: string; description?: string },
	): OtelHistogram;
}

interface OtelMetrics {
	getMeter(name: string, version?: string): OtelMeter;
}

// Try to import OpenTelemetry if available
let otelApi: typeof import("@opentelemetry/api") | undefined;
let otelMetrics: OtelMetrics | undefined;

try {
	// Dynamic import to make it truly optional
	otelApi = require("@opentelemetry/api");
} catch {
	// OpenTelemetry not installed, which is fine
}

try {
	// Try to load metrics API
	const metricsModule = require("@opentelemetry/api");
	otelMetrics = metricsModule.metrics;
} catch {
	// Metrics not available
}

export interface SpanOptions {
	name: string;
	attributes?: Record<string, string | number | boolean>;
}

export class ObservabilityHelper {
	private readonly logger?: LoggerService;
	private readonly logLevel: "debug" | "info" | "warn" | "error";
	private readonly tracingEnabled: boolean;
	private readonly metricsEnabled: boolean;
	private readonly meter?: OtelMeter;
	private readonly counters: Map<string, OtelCounter> = new Map();
	private readonly histograms: Map<string, OtelHistogram> = new Map();

	constructor(options?: ObservabilityOptions) {
		this.logger = options?.logging?.logger;
		this.logLevel = options?.logging?.level ?? "info";
		this.tracingEnabled = options?.tracing ?? false;
		this.metricsEnabled = options?.metrics ?? false;

		// Initialize meter if metrics are enabled and OpenTelemetry is available
		if (this.metricsEnabled && otelMetrics) {
			this.meter = otelMetrics.getMeter(
				"@prepit/nestjs-sqs-transporter",
				"0.3.3",
			);
		}
	}

	/**
	 * Create a span for tracing if OpenTelemetry is available and enabled
	 */
	createSpan<T>(
		spanOptions: SpanOptions,
		fn: () => T | Promise<T>,
	): T | Promise<T> {
		if (!this.tracingEnabled || !otelApi) {
			return fn();
		}

		// Capture the API reference to avoid non-null assertions in callbacks
		const api = otelApi;
		const tracer = api.trace.getTracer("@prepit/nestjs-sqs-transporter");
		return tracer.startActiveSpan(spanOptions.name, (span) => {
			if (spanOptions.attributes) {
				Object.entries(spanOptions.attributes).forEach(([key, value]) => {
					span.setAttribute(key, value);
				});
			}

			try {
				const result = fn();
				if (result instanceof Promise) {
					return result
						.then((res) => {
							span.setStatus({ code: api.SpanStatusCode.OK });
							span.end();
							return res;
						})
						.catch((err) => {
							span.setStatus({
								code: api.SpanStatusCode.ERROR,
								message: err.message,
							});
							span.recordException(err);
							span.end();
							throw err;
						});
				}
				span.setStatus({ code: api.SpanStatusCode.OK });
				span.end();
				return result;
			} catch (err) {
				const error = err as Error;
				span.setStatus({
					code: api.SpanStatusCode.ERROR,
					message: error.message,
				});
				span.recordException(error);
				span.end();
				throw err;
			}
		});
	}

	/**
	 * Log a message at the configured level
	 */
	log(message: string, context?: string): void {
		if (!this.logger) {
			return;
		}

		switch (this.logLevel) {
			case "debug":
				this.logger.debug?.(message, context);
				break;
			case "info":
				this.logger.log(message, context);
				break;
			case "warn":
				this.logger.warn(message, context);
				break;
			case "error":
				this.logger.error(message, undefined, context);
				break;
		}
	}

	/**
	 * Log an error
	 */
	logError(message: string, error: Error, context?: string): void {
		if (!this.logger) {
			return;
		}
		this.logger.error(message, error.stack, context);
	}

	/**
	 * Record a metric using OpenTelemetry if available, otherwise log as fallback.
	 * Duration metrics are recorded as histograms, count metrics as counters.
	 */
	recordMetric(
		name: string,
		value: number,
		attributes?: Record<string, string>,
	): void {
		if (!this.metricsEnabled) {
			return;
		}

		// Use OpenTelemetry metrics if meter is available
		if (this.meter) {
			// Use histogram for duration metrics, counter for everything else
			if (name.includes("duration") || name.includes("latency")) {
				let histogram = this.histograms.get(name);
				if (!histogram) {
					histogram = this.meter.createHistogram(name, {
						unit: "ms",
						description: `Duration metric for ${name}`,
					});
					this.histograms.set(name, histogram);
				}
				histogram.record(value, attributes);
			} else {
				let counter = this.counters.get(name);
				if (!counter) {
					counter = this.meter.createCounter(name, {
						description: `Counter metric for ${name}`,
					});
					this.counters.set(name, counter);
				}
				counter.add(value, attributes);
			}
			return;
		}

		// Fallback to logging if OpenTelemetry is not available
		this.log(
			`Metric: ${name}=${value} ${JSON.stringify(attributes ?? {})}`,
			"SqsMetrics",
		);
	}
}
