import { describe, expect, it } from "vitest";
import type { AWS } from "../../src/interfaces/sqs-options.interface";
import { SqsContext } from "../../src/server/sqs.context";

describe("SqsContext", () => {
	const createMessage = (
		overrides: Partial<AWS.SQS.Message> = {},
	): AWS.SQS.Message => ({
		MessageId: "msg-123",
		ReceiptHandle: "receipt-handle-456",
		Body: JSON.stringify({ data: "test" }),
		Attributes: {
			ApproximateReceiveCount: "2",
			SentTimestamp: "1234567890",
		},
		MessageAttributes: {
			customAttr: {
				StringValue: "customValue",
				DataType: "String",
			},
		},
		...overrides,
	});

	it("should return the original message", () => {
		const message = createMessage();
		const context = new SqsContext([message, "TEST_PATTERN"]);

		expect(context.getMessage()).toBe(message);
	});

	it("should return the pattern", () => {
		const message = createMessage();
		const context = new SqsContext([message, "TEST_PATTERN"]);

		expect(context.getPattern()).toBe("TEST_PATTERN");
	});

	it("should return the message ID", () => {
		const message = createMessage();
		const context = new SqsContext([message, "TEST_PATTERN"]);

		expect(context.getMessageId()).toBe("msg-123");
	});

	it("should return the receipt handle", () => {
		const message = createMessage();
		const context = new SqsContext([message, "TEST_PATTERN"]);

		expect(context.getReceiptHandle()).toBe("receipt-handle-456");
	});

	it("should return a message attribute value", () => {
		const message = createMessage();
		const context = new SqsContext([message, "TEST_PATTERN"]);

		expect(context.getMessageAttribute("customAttr")).toBe("customValue");
	});

	it("should return undefined for missing message attribute", () => {
		const message = createMessage();
		const context = new SqsContext([message, "TEST_PATTERN"]);

		expect(context.getMessageAttribute("nonExistent")).toBeUndefined();
	});

	it("should return an SQS attribute value", () => {
		const message = createMessage();
		const context = new SqsContext([message, "TEST_PATTERN"]);

		expect(context.getAttribute("SentTimestamp")).toBe("1234567890");
	});

	it("should return the approximate receive count", () => {
		const message = createMessage();
		const context = new SqsContext([message, "TEST_PATTERN"]);

		expect(context.getApproximateReceiveCount()).toBe(2);
	});

	it("should return 1 for missing approximate receive count", () => {
		const message = createMessage({ Attributes: {} });
		const context = new SqsContext([message, "TEST_PATTERN"]);

		expect(context.getApproximateReceiveCount()).toBe(1);
	});
});
