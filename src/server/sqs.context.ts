import { BaseRpcContext } from '@nestjs/microservices/ctx-host/base-rpc.context';
import type { AWS } from '../interfaces/sqs-options.interface';

type SqsContextArgs = [AWS.SQS.Message, string];

export class SqsContext extends BaseRpcContext<SqsContextArgs> {
  /**
   * Returns the original SQS message
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
    const count = this.getAttribute('ApproximateReceiveCount');
    return count ? Number.parseInt(count, 10) : 1;
  }
}
