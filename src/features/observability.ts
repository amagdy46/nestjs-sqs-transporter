import type { LoggerService } from '@nestjs/common';
import type { ObservabilityOptions } from '../interfaces/sqs-options.interface';

// Try to import OpenTelemetry if available
let otelApi: typeof import('@opentelemetry/api') | undefined;
try {
  // Dynamic import to make it truly optional
  otelApi = require('@opentelemetry/api');
} catch {
  // OpenTelemetry not installed, which is fine
}

export interface SpanOptions {
  name: string;
  attributes?: Record<string, string | number | boolean>;
}

export class ObservabilityHelper {
  private readonly logger?: LoggerService;
  private readonly logLevel: 'debug' | 'info' | 'warn' | 'error';
  private readonly tracingEnabled: boolean;
  private readonly metricsEnabled: boolean;

  constructor(options?: ObservabilityOptions) {
    this.logger = options?.logging?.logger;
    this.logLevel = options?.logging?.level ?? 'info';
    this.tracingEnabled = options?.tracing ?? false;
    this.metricsEnabled = options?.metrics ?? false;
  }

  /**
   * Create a span for tracing if OpenTelemetry is available and enabled
   */
  createSpan<T>(spanOptions: SpanOptions, fn: () => T | Promise<T>): T | Promise<T> {
    if (!this.tracingEnabled || !otelApi) {
      return fn();
    }

    const tracer = otelApi.trace.getTracer('@prepit/nestjs-sqs-transporter');
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
              span.setStatus({ code: otelApi!.SpanStatusCode.OK });
              span.end();
              return res;
            })
            .catch((err) => {
              span.setStatus({
                code: otelApi!.SpanStatusCode.ERROR,
                message: err.message,
              });
              span.recordException(err);
              span.end();
              throw err;
            });
        }
        span.setStatus({ code: otelApi!.SpanStatusCode.OK });
        span.end();
        return result;
      } catch (err) {
        const error = err as Error;
        span.setStatus({
          code: otelApi!.SpanStatusCode.ERROR,
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
      case 'debug':
        this.logger.debug?.(message, context);
        break;
      case 'info':
        this.logger.log(message, context);
        break;
      case 'warn':
        this.logger.warn(message, context);
        break;
      case 'error':
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
   * Record a metric (placeholder for future implementation)
   */
  recordMetric(name: string, value: number, attributes?: Record<string, string>): void {
    if (!this.metricsEnabled) {
      return;
    }

    // TODO: Implement metrics when OpenTelemetry metrics are needed
    // For now, just log the metric
    this.log(`Metric: ${name}=${value} ${JSON.stringify(attributes ?? {})}`, 'SqsMetrics');
  }
}
