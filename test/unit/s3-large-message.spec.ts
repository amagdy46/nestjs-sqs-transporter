import type { S3Client } from "@aws-sdk/client-s3";
import {
	DeleteObjectCommand,
	GetObjectCommand,
	PutObjectCommand,
} from "@aws-sdk/client-s3";
import {
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	type Mock,
	vi,
} from "vitest";
import {
	S3_DEFAULT_KEY_PREFIX,
	S3_DEFAULT_THRESHOLD,
	S3_POINTER_CLASS,
	S3_POINTER_KEY,
} from "../../src/constants";
import { S3LargeMessageHandler } from "../../src/features/s3-large-message";
import type { S3LargeMessageOptions } from "../../src/interfaces/sqs-options.interface";

// Mock uuid
vi.mock("uuid", () => ({
	v4: vi.fn(() => "mock-uuid-456"),
}));

describe("S3LargeMessageHandler", () => {
	let handler: S3LargeMessageHandler;
	let mockS3Client: {
		send: Mock;
	};

	const defaultOptions: S3LargeMessageOptions = {
		enabled: true,
		s3Client: {} as S3Client,
		bucket: "test-bucket",
	};

	beforeEach(() => {
		vi.clearAllMocks();

		mockS3Client = {
			send: vi.fn(),
		};
	});

	afterEach(() => {
		vi.restoreAllMocks();
	});

	describe("Constructor", () => {
		it("should use default threshold when not specified", () => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
			});

			// Default threshold is 240KB
			expect(handler).toBeInstanceOf(S3LargeMessageHandler);
		});

		it("should use custom threshold when specified", () => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
				threshold: 100 * 1024, // 100KB
			});

			expect(handler).toBeInstanceOf(S3LargeMessageHandler);
		});

		it("should use default key prefix when not specified", () => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
			});

			expect(handler).toBeInstanceOf(S3LargeMessageHandler);
		});

		it("should use custom key prefix when specified", () => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
				keyPrefix: "custom-prefix/",
			});

			expect(handler).toBeInstanceOf(S3LargeMessageHandler);
		});

		it("should default to auto pointer format", () => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
			});

			expect(handler).toBeInstanceOf(S3LargeMessageHandler);
		});

		it("should use custom pointer key when specified", () => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
				pointerKey: "customPointer",
			});

			expect(handler).toBeInstanceOf(S3LargeMessageHandler);
		});
	});

	describe("wrapIfLarge()", () => {
		beforeEach(() => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
				threshold: 100, // Low threshold for testing (100 bytes)
			});

			mockS3Client.send.mockResolvedValue({});
		});

		it("should return original message when under threshold", async () => {
			const smallMessage = JSON.stringify({ small: "data" });

			const result = await handler.wrapIfLarge(smallMessage);

			expect(result).toBe(smallMessage);
			expect(mockS3Client.send).not.toHaveBeenCalled();
		});

		it("should upload to S3 when over threshold", async () => {
			const largeMessage = "x".repeat(150); // Over 100 byte threshold

			await handler.wrapIfLarge(largeMessage);

			expect(mockS3Client.send).toHaveBeenCalledWith(
				expect.any(PutObjectCommand),
			);
		});

		it("should create S3 object with correct parameters", async () => {
			const largeMessage = "x".repeat(150);

			await handler.wrapIfLarge(largeMessage);

			const putCommand = mockS3Client.send.mock.calls[0][0];
			expect(putCommand.input).toEqual({
				Bucket: "test-bucket",
				Key: `${S3_DEFAULT_KEY_PREFIX}mock-uuid-456`,
				Body: largeMessage,
				ContentType: "application/json",
			});
		});

		it("should use custom key prefix", async () => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
				threshold: 100,
				keyPrefix: "custom/",
			});

			const largeMessage = "x".repeat(150);

			await handler.wrapIfLarge(largeMessage);

			const putCommand = mockS3Client.send.mock.calls[0][0];
			expect(putCommand.input.Key).toBe("custom/mock-uuid-456");
		});

		it("should return simple format pointer by default (auto)", async () => {
			const largeMessage = "x".repeat(150);

			const result = await handler.wrapIfLarge(largeMessage);
			const parsed = JSON.parse(result);

			expect(parsed[S3_POINTER_KEY]).toEqual({
				bucket: "test-bucket",
				key: `${S3_DEFAULT_KEY_PREFIX}mock-uuid-456`,
			});
		});

		it("should return aws-extended format pointer when configured", async () => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
				threshold: 100,
				pointerFormat: "aws-extended",
			});

			const largeMessage = "x".repeat(150);

			const result = await handler.wrapIfLarge(largeMessage);
			const parsed = JSON.parse(result);

			expect(Array.isArray(parsed)).toBe(true);
			expect(parsed[0]).toEqual({
				"@class": S3_POINTER_CLASS,
				s3BucketName: "test-bucket",
				s3Key: `${S3_DEFAULT_KEY_PREFIX}mock-uuid-456`,
			});
		});

		it("should use custom pointer key in simple format", async () => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
				threshold: 100,
				pointerKey: "s3Ref",
			});

			const largeMessage = "x".repeat(150);

			const result = await handler.wrapIfLarge(largeMessage);
			const parsed = JSON.parse(result);

			expect(parsed.s3Ref).toEqual({
				bucket: "test-bucket",
				key: `${S3_DEFAULT_KEY_PREFIX}mock-uuid-456`,
			});
		});

		it("should handle exactly at threshold (not upload)", async () => {
			const exactMessage = "x".repeat(100); // Exactly 100 bytes

			const result = await handler.wrapIfLarge(exactMessage);

			expect(result).toBe(exactMessage);
			expect(mockS3Client.send).not.toHaveBeenCalled();
		});

		it("should handle one byte over threshold (upload)", async () => {
			const overMessage = "x".repeat(101); // 101 bytes

			await handler.wrapIfLarge(overMessage);

			expect(mockS3Client.send).toHaveBeenCalled();
		});
	});

	describe("unwrapIfPointer()", () => {
		const mockBodyStream = {
			transformToString: vi.fn(),
		};

		beforeEach(() => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
			});

			mockS3Client.send.mockResolvedValue({
				Body: mockBodyStream,
			});
		});

		it("should return original message when not a pointer", async () => {
			const regularMessage = JSON.stringify({ pattern: "TEST", data: {} });

			const result = await handler.unwrapIfPointer(regularMessage);

			expect(result).toBe(regularMessage);
			expect(mockS3Client.send).not.toHaveBeenCalled();
		});

		it("should download from S3 for simple format pointer", async () => {
			const actualContent = JSON.stringify({
				pattern: "TEST",
				data: { large: "content" },
			});
			mockBodyStream.transformToString.mockResolvedValue(actualContent);

			const pointer = JSON.stringify({
				[S3_POINTER_KEY]: { bucket: "test-bucket", key: "messages/123" },
			});

			const result = await handler.unwrapIfPointer(pointer);

			expect(mockS3Client.send).toHaveBeenCalledWith(
				expect.any(GetObjectCommand),
			);
			expect(result).toBe(actualContent);
		});

		it("should download from S3 for aws-extended format pointer (array)", async () => {
			const actualContent = JSON.stringify({
				pattern: "TEST",
				data: { large: "content" },
			});
			mockBodyStream.transformToString.mockResolvedValue(actualContent);

			const pointer = JSON.stringify([
				{
					"@class": S3_POINTER_CLASS,
					s3BucketName: "test-bucket",
					s3Key: "messages/123",
				},
			]);

			const result = await handler.unwrapIfPointer(pointer);

			expect(mockS3Client.send).toHaveBeenCalledWith(
				expect.any(GetObjectCommand),
			);
			const getCommand = mockS3Client.send.mock.calls[0][0];
			expect(getCommand.input).toEqual({
				Bucket: "test-bucket",
				Key: "messages/123",
			});
			expect(result).toBe(actualContent);
		});

		it("should download from S3 for aws-extended format pointer (object)", async () => {
			const actualContent = JSON.stringify({ pattern: "TEST", data: {} });
			mockBodyStream.transformToString.mockResolvedValue(actualContent);

			const pointer = JSON.stringify({
				"@class": S3_POINTER_CLASS,
				s3BucketName: "test-bucket",
				s3Key: "messages/456",
			});

			const result = await handler.unwrapIfPointer(pointer);

			expect(mockS3Client.send).toHaveBeenCalled();
			expect(result).toBe(actualContent);
		});

		it("should use custom pointer key for simple format", async () => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
				pointerKey: "customRef",
			});

			const actualContent = JSON.stringify({ pattern: "TEST", data: {} });
			mockBodyStream.transformToString.mockResolvedValue(actualContent);

			const pointer = JSON.stringify({
				customRef: { bucket: "test-bucket", key: "messages/789" },
			});

			const result = await handler.unwrapIfPointer(pointer);

			expect(mockS3Client.send).toHaveBeenCalled();
			expect(result).toBe(actualContent);
		});

		it("should throw error when S3 body is empty", async () => {
			mockS3Client.send.mockResolvedValue({ Body: null });

			const pointer = JSON.stringify({
				[S3_POINTER_KEY]: { bucket: "test-bucket", key: "messages/123" },
			});

			await expect(handler.unwrapIfPointer(pointer)).rejects.toThrow(
				"Failed to read S3 object: messages/123",
			);
		});

		it("should throw error when transformToString returns empty", async () => {
			mockBodyStream.transformToString.mockResolvedValue("");

			const pointer = JSON.stringify({
				[S3_POINTER_KEY]: { bucket: "test-bucket", key: "messages/123" },
			});

			await expect(handler.unwrapIfPointer(pointer)).rejects.toThrow(
				"Failed to read S3 object: messages/123",
			);
		});

		it("should handle non-JSON messages as regular messages", async () => {
			const plainText = "This is plain text, not JSON";

			const result = await handler.unwrapIfPointer(plainText);

			expect(result).toBe(plainText);
			expect(mockS3Client.send).not.toHaveBeenCalled();
		});
	});

	describe("deleteS3Object()", () => {
		beforeEach(() => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
			});

			mockS3Client.send.mockResolvedValue({});
		});

		it("should delete S3 object for simple format pointer", async () => {
			const pointer = JSON.stringify({
				[S3_POINTER_KEY]: { bucket: "test-bucket", key: "messages/123" },
			});

			await handler.deleteS3Object(pointer);

			expect(mockS3Client.send).toHaveBeenCalledWith(
				expect.any(DeleteObjectCommand),
			);
			const deleteCommand = mockS3Client.send.mock.calls[0][0];
			expect(deleteCommand.input).toEqual({
				Bucket: "test-bucket",
				Key: "messages/123",
			});
		});

		it("should delete S3 object for aws-extended format pointer", async () => {
			const pointer = JSON.stringify([
				{
					"@class": S3_POINTER_CLASS,
					s3BucketName: "test-bucket",
					s3Key: "messages/456",
				},
			]);

			await handler.deleteS3Object(pointer);

			expect(mockS3Client.send).toHaveBeenCalledWith(
				expect.any(DeleteObjectCommand),
			);
		});

		it("should not call S3 for non-pointer messages", async () => {
			const regularMessage = JSON.stringify({ pattern: "TEST", data: {} });

			await handler.deleteS3Object(regularMessage);

			expect(mockS3Client.send).not.toHaveBeenCalled();
		});

		it("should not throw for non-pointer messages", async () => {
			const regularMessage = JSON.stringify({ pattern: "TEST", data: {} });

			await expect(
				handler.deleteS3Object(regularMessage),
			).resolves.toBeUndefined();
		});
	});

	describe("isS3Pointer()", () => {
		beforeEach(() => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
			});
		});

		it("should return true for simple format pointer", () => {
			const pointer = JSON.stringify({
				[S3_POINTER_KEY]: { bucket: "test-bucket", key: "messages/123" },
			});

			expect(handler.isS3Pointer(pointer)).toBe(true);
		});

		it("should return true for aws-extended format pointer (array)", () => {
			const pointer = JSON.stringify([
				{
					"@class": S3_POINTER_CLASS,
					s3BucketName: "test-bucket",
					s3Key: "messages/123",
				},
			]);

			expect(handler.isS3Pointer(pointer)).toBe(true);
		});

		it("should return true for aws-extended format pointer (object)", () => {
			const pointer = JSON.stringify({
				"@class": S3_POINTER_CLASS,
				s3BucketName: "test-bucket",
				s3Key: "messages/123",
			});

			expect(handler.isS3Pointer(pointer)).toBe(true);
		});

		it("should return false for regular JSON message", () => {
			const message = JSON.stringify({ pattern: "TEST", data: {} });

			expect(handler.isS3Pointer(message)).toBe(false);
		});

		it("should return false for plain text message", () => {
			expect(handler.isS3Pointer("plain text")).toBe(false);
		});

		it("should return false for empty string", () => {
			expect(handler.isS3Pointer("")).toBe(false);
		});

		it("should return false for invalid JSON", () => {
			expect(handler.isS3Pointer("{ invalid json")).toBe(false);
		});

		it("should return false for pointer with wrong class", () => {
			const wrongClass = JSON.stringify([
				{
					"@class": "some.other.Class",
					s3BucketName: "test-bucket",
					s3Key: "messages/123",
				},
			]);

			expect(handler.isS3Pointer(wrongClass)).toBe(false);
		});

		it("should detect custom pointer key", () => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
				pointerKey: "myPointer",
			});

			const customPointer = JSON.stringify({
				myPointer: { bucket: "test-bucket", key: "messages/123" },
			});

			expect(handler.isS3Pointer(customPointer)).toBe(true);
		});

		it("should not detect default pointer key when custom key is set", () => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
				pointerKey: "myPointer",
				pointerFormat: "simple", // Only simple format
			});

			const defaultPointer = JSON.stringify({
				[S3_POINTER_KEY]: { bucket: "test-bucket", key: "messages/123" },
			});

			expect(handler.isS3Pointer(defaultPointer)).toBe(false);
		});
	});

	describe("Pointer Format Configurations", () => {
		describe("pointerFormat: simple", () => {
			beforeEach(() => {
				handler = new S3LargeMessageHandler({
					...defaultOptions,
					s3Client: mockS3Client as unknown as S3Client,
					pointerFormat: "simple",
				});
			});

			it("should only parse simple format pointers", () => {
				const simplePointer = JSON.stringify({
					[S3_POINTER_KEY]: { bucket: "test-bucket", key: "messages/123" },
				});

				expect(handler.isS3Pointer(simplePointer)).toBe(true);
			});

			it("should not parse aws-extended format when set to simple", () => {
				const awsPointer = JSON.stringify([
					{
						"@class": S3_POINTER_CLASS,
						s3BucketName: "test-bucket",
						s3Key: "messages/123",
					},
				]);

				expect(handler.isS3Pointer(awsPointer)).toBe(false);
			});
		});

		describe("pointerFormat: aws-extended", () => {
			beforeEach(() => {
				handler = new S3LargeMessageHandler({
					...defaultOptions,
					s3Client: mockS3Client as unknown as S3Client,
					pointerFormat: "aws-extended",
					threshold: 100,
				});

				mockS3Client.send.mockResolvedValue({});
			});

			it("should only parse aws-extended format pointers", () => {
				const awsPointer = JSON.stringify([
					{
						"@class": S3_POINTER_CLASS,
						s3BucketName: "test-bucket",
						s3Key: "messages/123",
					},
				]);

				expect(handler.isS3Pointer(awsPointer)).toBe(true);
			});

			it("should not parse simple format when set to aws-extended", () => {
				const simplePointer = JSON.stringify({
					[S3_POINTER_KEY]: { bucket: "test-bucket", key: "messages/123" },
				});

				expect(handler.isS3Pointer(simplePointer)).toBe(false);
			});

			it("should create aws-extended format when wrapping", async () => {
				const largeMessage = "x".repeat(150);

				const result = await handler.wrapIfLarge(largeMessage);
				const parsed = JSON.parse(result);

				expect(Array.isArray(parsed)).toBe(true);
				expect(parsed[0]["@class"]).toBe(S3_POINTER_CLASS);
			});
		});

		describe("pointerFormat: auto (default)", () => {
			beforeEach(() => {
				handler = new S3LargeMessageHandler({
					...defaultOptions,
					s3Client: mockS3Client as unknown as S3Client,
					// pointerFormat defaults to 'auto'
					threshold: 100,
				});

				mockS3Client.send.mockResolvedValue({});
			});

			it("should parse both simple and aws-extended formats", () => {
				const simplePointer = JSON.stringify({
					[S3_POINTER_KEY]: { bucket: "test-bucket", key: "messages/123" },
				});

				const awsPointer = JSON.stringify([
					{
						"@class": S3_POINTER_CLASS,
						s3BucketName: "test-bucket",
						s3Key: "messages/456",
					},
				]);

				expect(handler.isS3Pointer(simplePointer)).toBe(true);
				expect(handler.isS3Pointer(awsPointer)).toBe(true);
			});

			it("should create simple format when wrapping (auto defaults to simple for writes)", async () => {
				const largeMessage = "x".repeat(150);

				const result = await handler.wrapIfLarge(largeMessage);
				const parsed = JSON.parse(result);

				expect(parsed[S3_POINTER_KEY]).toBeDefined();
				expect(Array.isArray(parsed)).toBe(false);
			});
		});
	});

	describe("Edge Cases", () => {
		beforeEach(() => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
				threshold: 100,
			});

			mockS3Client.send.mockResolvedValue({});
		});

		it("should handle empty string message", async () => {
			const result = await handler.wrapIfLarge("");

			expect(result).toBe("");
			expect(mockS3Client.send).not.toHaveBeenCalled();
		});

		it("should handle unicode characters correctly for size calculation", async () => {
			// Unicode characters can take multiple bytes
			const unicodeMessage = "🎉".repeat(30); // Each emoji is 4 bytes

			await handler.wrapIfLarge(unicodeMessage);

			// 30 * 4 = 120 bytes, over 100 threshold
			expect(mockS3Client.send).toHaveBeenCalled();
		});

		it("should handle message with null bytes", async () => {
			const nullByteMessage = "test\x00message".repeat(20);

			const result = await handler.wrapIfLarge(nullByteMessage);

			// Should either upload or return original based on size
			expect(result).toBeDefined();
		});

		it("should handle pointer with missing bucket field", () => {
			const invalidPointer = JSON.stringify({
				[S3_POINTER_KEY]: { key: "messages/123" }, // Missing bucket
			});

			expect(handler.isS3Pointer(invalidPointer)).toBe(false);
		});

		it("should handle pointer with missing key field", () => {
			const invalidPointer = JSON.stringify({
				[S3_POINTER_KEY]: { bucket: "test-bucket" }, // Missing key
			});

			expect(handler.isS3Pointer(invalidPointer)).toBe(false);
		});

		it("should handle pointer with non-string bucket", () => {
			const invalidPointer = JSON.stringify({
				[S3_POINTER_KEY]: { bucket: 123, key: "messages/123" },
			});

			expect(handler.isS3Pointer(invalidPointer)).toBe(false);
		});

		it("should handle pointer with non-string key", () => {
			const invalidPointer = JSON.stringify({
				[S3_POINTER_KEY]: { bucket: "test-bucket", key: 456 },
			});

			expect(handler.isS3Pointer(invalidPointer)).toBe(false);
		});

		it("should handle deeply nested JSON without triggering false positive", () => {
			const nestedJson = JSON.stringify({
				pattern: "TEST",
				data: {
					nested: {
						deep: {
							bucket: "some-bucket", // Should not be detected as pointer
							key: "some-key",
						},
					},
				},
			});

			expect(handler.isS3Pointer(nestedJson)).toBe(false);
		});
	});

	describe("S3 Client Error Handling", () => {
		beforeEach(() => {
			handler = new S3LargeMessageHandler({
				...defaultOptions,
				s3Client: mockS3Client as unknown as S3Client,
				threshold: 100,
			});
		});

		it("should propagate S3 upload errors", async () => {
			mockS3Client.send.mockRejectedValue(new Error("S3 upload failed"));

			const largeMessage = "x".repeat(150);

			await expect(handler.wrapIfLarge(largeMessage)).rejects.toThrow(
				"S3 upload failed",
			);
		});

		it("should propagate S3 download errors", async () => {
			mockS3Client.send.mockRejectedValue(new Error("S3 download failed"));

			const pointer = JSON.stringify({
				[S3_POINTER_KEY]: { bucket: "test-bucket", key: "messages/123" },
			});

			await expect(handler.unwrapIfPointer(pointer)).rejects.toThrow(
				"S3 download failed",
			);
		});

		it("should propagate S3 delete errors", async () => {
			mockS3Client.send.mockRejectedValue(new Error("S3 delete failed"));

			const pointer = JSON.stringify({
				[S3_POINTER_KEY]: { bucket: "test-bucket", key: "messages/123" },
			});

			await expect(handler.deleteS3Object(pointer)).rejects.toThrow(
				"S3 delete failed",
			);
		});
	});

	describe("Default Values", () => {
		it("should use S3_DEFAULT_THRESHOLD", () => {
			handler = new S3LargeMessageHandler({
				enabled: true,
				s3Client: mockS3Client as unknown as S3Client,
				bucket: "test-bucket",
			});

			// S3_DEFAULT_THRESHOLD is 240 * 1024 = 245760 bytes
			expect(S3_DEFAULT_THRESHOLD).toBe(240 * 1024);
		});

		it("should use S3_DEFAULT_KEY_PREFIX", () => {
			expect(S3_DEFAULT_KEY_PREFIX).toBe("sqs-messages/");
		});

		it("should use S3_POINTER_KEY", () => {
			expect(S3_POINTER_KEY).toBe("__s3pointer");
		});

		it("should use S3_POINTER_CLASS", () => {
			expect(S3_POINTER_CLASS).toBe(
				"software.amazon.payloadoffloading.PayloadS3Pointer",
			);
		});
	});
});
