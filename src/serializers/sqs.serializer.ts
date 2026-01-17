import type { Serializer } from "@nestjs/microservices";

export interface SqsSerializedMessage {
	pattern: string;
	data: unknown;
	id?: string;
}

export interface SqsSerializerOptions {
	/**
	 * Custom key name for the message pattern field.
	 * @default 'pattern'
	 * @example 'type' -> serializes as { type: 'ORDER_CREATED', data: {...} }
	 */
	patternKey?: string;
}

export class SqsSerializer implements Serializer<unknown, string> {
	private readonly patternKey: string;

	constructor(options?: SqsSerializerOptions) {
		this.patternKey = options?.patternKey ?? "pattern";
	}

	serialize(packet: SqsSerializedMessage): string {
		return JSON.stringify({
			[this.patternKey]: packet.pattern,
			data: packet.data,
			id: packet.id,
		});
	}
}
