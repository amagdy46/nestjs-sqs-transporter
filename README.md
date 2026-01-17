# NestJS SQS Transporter

NestJS microservice transporter for AWS SQS following official patterns.

Tested with: AWS SQS and LocalStack.

## Features

- `@EventPattern` decorator (fire-and-forget)
- S3 large message support (>256KB)
- FIFO queue support with flexible configuration
- OpenTelemetry observability (optional)
- Testing utilities (MockServerSqs, MockClientSqs)

## Installation

```bash
npm i nestjs-sqs-transporter
```

For S3 large message support:
```bash
npm i @aws-sdk/client-s3
```

## Quick Start

### Register the transporter

```typescript
// main.ts
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions } from '@nestjs/microservices';
import { SQSClient } from '@aws-sdk/client-sqs';
import { ServerSqs } from 'nestjs-sqs-transporter';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.connectMicroservice<MicroserviceOptions>({
    strategy: new ServerSqs({
      sqs: new SQSClient({ region: 'us-east-1' }),
      consumerOptions: {
        queueUrl: process.env.SQS_QUEUE_URL,
      },
    }),
  });

  await app.startAllMicroservices();
  await app.listen(3000);
}
bootstrap();
```

### Handle messages

```typescript
import { Controller } from '@nestjs/common';
import { EventPattern, Payload, Ctx } from '@nestjs/microservices';
import { SqsContext } from 'nestjs-sqs-transporter';

@Controller()
export class MessageController {
  @EventPattern('ORDER_CREATED')
  async handleOrderCreated(
    @Payload() data: OrderDto,
    @Ctx() context: SqsContext,
  ) {
    console.log(`Processing message ${context.getMessageId()}`);
    // Process the order...
  }
}
```

### Send messages

```typescript
// app.module.ts
import { Module } from '@nestjs/common';
import { ClientsModule } from '@nestjs/microservices';
import { SQSClient } from '@aws-sdk/client-sqs';
import { ClientSqs } from 'nestjs-sqs-transporter';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'SQS_SERVICE',
        customClass: ClientSqs,
        options: {
          sqs: new SQSClient({ region: 'us-east-1' }),
          queueUrl: process.env.SQS_QUEUE_URL,
        },
      },
    ]),
  ],
})
export class AppModule {}
```

```typescript
// order.service.ts
import { Injectable, Inject } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';

@Injectable()
export class OrderService {
  constructor(@Inject('SQS_SERVICE') private sqsClient: ClientProxy) {}

  async createOrder(order: OrderDto) {
    this.sqsClient.emit('ORDER_CREATED', order);
  }
}
```

## Advanced Features

### S3 Large Messages

For messages larger than 256KB:

```typescript
import { S3Client } from '@aws-sdk/client-s3';

// Server
new ServerSqs({
  sqs: new SQSClient({ region: 'us-east-1' }),
  consumerOptions: { queueUrl: '...' },
  s3LargeMessage: {
    enabled: true,
    s3Client: new S3Client({ region: 'us-east-1' }),
    bucket: 'my-large-messages-bucket',
    // Optional: customize the pointer key (default: '__s3pointer')
    pointerKey: 's3Pointer', // Results in { s3Pointer: { bucket, key } }
  },
});

// Client
{
  customClass: ClientSqs,
  options: {
    sqs: new SQSClient({ region: 'us-east-1' }),
    queueUrl: '...',
    s3LargeMessage: {
      enabled: true,
      s3Client: new S3Client({ region: 'us-east-1' }),
      bucket: 'my-large-messages-bucket',
      pointerKey: 's3Pointer', // Must match server config
    },
  },
}
```

### FIFO Queues

```typescript
{
  customClass: ClientSqs,
  options: {
    sqs: new SQSClient({ region: 'us-east-1' }),
    queueUrl: 'https://sqs.../my-queue.fifo',
    fifo: {
      enabled: true,
      messageGroupId: (pattern, data) => data.customerId,
      deduplicationId: (pattern, data) => `${pattern}-${data.orderId}`,
    },
  },
}
```

### Observability

```typescript
new ServerSqs({
  // ...
  observability: {
    tracing: true,
    metrics: true,
    logging: { level: 'debug' },
  },
});
```

### Testing

```typescript
import { MockClientSqs } from 'nestjs-sqs-transporter';

describe('OrderService', () => {
  let mockClient: MockClientSqs;

  beforeEach(() => {
    mockClient = new MockClientSqs();
  });

  it('should emit order created event', async () => {
    const service = new OrderService(mockClient);
    await service.createOrder({ id: '123' });

    const event = mockClient.expectEventEmitted('ORDER_CREATED');
    expect(event.data.id).toBe('123');
  });
});
```

## API Reference

### ServerSqs Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `sqs` | `SQSClient` | Yes | AWS SQS client |
| `consumerOptions.queueUrl` | `string` | Yes | Queue URL |
| `consumerOptions.waitTimeSeconds` | `number` | No | Long poll wait (default: 20) |
| `consumerOptions.batchSize` | `number` | No | Messages per poll (default: 10) |
| `s3LargeMessage` | `object` | No | S3 offloading config |
| `observability` | `object` | No | Tracing/metrics config |

### ClientSqs Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `sqs` | `SQSClient` | Yes | AWS SQS client |
| `queueUrl` | `string` | Yes | Target queue URL |
| `fifo` | `object` | No | FIFO queue config |
| `s3LargeMessage` | `object` | No | S3 offloading config |
| `observability` | `object` | No | Tracing/metrics config |

### SqsContext Methods

| Method | Description |
|--------|-------------|
| `getMessage()` | Original SQS message |
| `getMessageId()` | SQS message ID |
| `getPattern()` | Message pattern |
| `getReceiptHandle()` | Receipt handle |
| `getApproximateReceiveCount()` | Receive count |

## License

MIT
