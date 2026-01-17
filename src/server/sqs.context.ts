import { BaseRpcContext } from "@nestjs/microservices/ctx-host/base-rpc.context";
import type { AWS } from "../interfaces/sqs-options.interface";

type SqsContextArgs = [AWS.SQS.Message, string];

/**
 * SQS context passed to message handlers.
 *
 * Provides access to the original SQS message, its metadata, and attributes.
 * Use the `@Ctx()` decorator to inject this context into your handlers.
 *
 * @example
 * ```typescript
 * @EventPattern('ORDER_CREATED')
 * async handleOrder(data: OrderData, @Ctx() ctx: SqsContext) {
 *   // Get the full SQS message
 *   const message = ctx.getMessage();
 *
 *   // Get specific metadata
 *   console.log('Message ID:', ctx.getMessageId());
 *   console.log('Pattern:', ctx.getPattern());
 *   console.log('Receive count:', ctx.getApproximateReceiveCount());
 *
 *   // Access custom message attributes
 *   const traceId = ctx.getMessageAttribute('X-Trace-Id');
 * }
 * ```
 */
export class SqsContext extends BaseRpcContext<SqsContextArgs> {
	/**
	 * Returns the original SQS message.
	 *
	 * The message body will be unwrapped if it was an S3 pointer (when S3 large
	 * message handling is enabled on the server).
	 *
	 * @returns The full SQS message object with all attributes
	 */
	getMessage(): AWS.SQS.Message {
		return this.args[0];
	}

	/**
	 * Returns the message pattern (event/message type)
	 */
	getPattern(): string {
		return this.args[1];
	}

	/**
	 * Returns the SQS message ID
	 */
	getMessageId(): string | undefined {
		return this.getMessage().MessageId;
	}

	/**
	 * Returns the receipt handle for message deletion
	 */
	getReceiptHandle(): string | undefined {
		return this.getMessage().ReceiptHandle;
	}

	/**
	 * Returns a specific message attribute value
	 */
	getMessageAttribute(name: string): string | undefined {
		return this.getMessage().MessageAttributes?.[name]?.StringValue;
	}

	/**
	 * Returns a specific SQS attribute (e.g., ApproximateReceiveCount)
	 */
	getAttribute(name: string): string | undefined {
		return this.getMessage().Attributes?.[name];
	}

	/**
	 * Returns the approximate receive count for this message
	 */
	getApproximateReceiveCount(): number {
		const count = this.getAttribute("ApproximateReceiveCount");
		return count ? Number.parseInt(count, 10) : 1;
	}
}
