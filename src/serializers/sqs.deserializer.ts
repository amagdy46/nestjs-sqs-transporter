import type {
	Deserializer,
	IncomingEvent,
	IncomingRequest,
} from "@nestjs/microservices";
import { CORRELATION_ID_HEADER, MESSAGE_PATTERN_HEADER } from "../constants";
import type { AWS } from "../interfaces/sqs-options.interface";

export interface SqsDeserializedMessage {
	pattern: string;
	data: unknown;
	id?: string;
}

export interface SqsDeserializerOptions {
	/**
	 * Custom key name for the message pattern field.
	 * @default 'pattern'
	 * @example 'type' -> reads pattern from { type: 'ORDER_CREATED', data: {...} }
	 */
	patternKey?: string;
}

export class SqsDeserializer
	implements Deserializer<AWS.SQS.Message, IncomingRequest | IncomingEvent>
{
	private readonly patternKey: string;

	constructor(options?: SqsDeserializerOptions) {
		this.patternKey = options?.patternKey ?? "pattern";
	}

	deserialize(message: AWS.SQS.Message): IncomingRequest | IncomingEvent {
		const body = message.Body;
		if (!body) {
			throw new Error("Message body is empty");
		}

		let parsed: Record<string, unknown>;

		try {
			parsed = JSON.parse(body);
		} catch {
			// If body is not JSON, treat it as raw data
			// Try to get pattern from message attributes
			const pattern =
				message.MessageAttributes?.[MESSAGE_PATTERN_HEADER]?.StringValue ||
				"unknown";
			return {
				pattern,
				data: body,
			};
		}

		// If parsed object has patternKey field, use it as pattern
		const patternValue = parsed[this.patternKey];
		if (patternValue !== undefined) {
			const correlationId =
				message.MessageAttributes?.[CORRELATION_ID_HEADER]?.StringValue ||
				(parsed.id as string);

			// For standard NestJS format (patternKey='pattern'), data is in parsed.data
			// For custom formats (e.g., patternKey='type'), the whole message is the data
			const data =
				this.patternKey === "pattern" ? (parsed.data ?? parsed) : parsed;

			return {
				pattern: patternValue as string,
				data,
				id: correlationId,
			};
		}

		// Otherwise, try to extract pattern from message attributes
		const pattern =
			message.MessageAttributes?.[MESSAGE_PATTERN_HEADER]?.StringValue ||
			"unknown";
		return {
			pattern,
			data: parsed,
		};
	}
}
