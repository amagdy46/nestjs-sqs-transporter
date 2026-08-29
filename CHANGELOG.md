# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Route lifecycle and error events exclusively through the configured logger, with
  `log` as the fallback when a logger does not implement optional debug logging.
- Send metrics only through OpenTelemetry and, when metrics are unavailable, emit
  at most one warning through a configured logger instead of logging every metric.

## [0.4.1] - 2025-01-23

### Changed
- Enhanced README with badges, logo, and table of contents
- Improved package.json SEO with additional keywords
- Switched to OIDC trusted publisher for npm releases (no token rotation needed)

### Added
- CHANGELOG.md for version history
- CONTRIBUTING.md for contributor guidelines
- Codecov integration for coverage and test analytics
- GitHub Release creation on publish

### Fixed
- Vitest ESM error by renaming config to .mts extension

## [0.4.0] - 2025-01-17

### Added
- Comprehensive test coverage across all modules
- `patternKey` option for custom message routing field (e.g., `type` instead of `pattern`)
- `pointerKey` option for S3 large messages to customize the pointer field name

### Changed
- When using custom `patternKey`, the entire message is now passed as data for external system compatibility

### Fixed
- Fixed message handling when `patternKey` is used with external message formats

## [0.3.0] - 2025-01-20

### Added
- S3 large message support for payloads exceeding 256KB
- Automatic upload/download of large payloads to/from S3
- Configurable S3 bucket and pointer key

### Changed
- Improved message serialization for large payload detection

## [0.2.0] - 2025-01-15

### Added
- FIFO queue support with `messageGroupId` and `deduplicationId` options
- Support for dynamic group ID and deduplication ID functions
- MockClientSqs and MockServerSqs for testing

### Changed
- Enhanced SqsContext with additional methods for message inspection

## [0.1.0] - 2025-01-10

### Added
- Initial release
- ServerSqs - NestJS microservice server for SQS consumers
- ClientSqs - NestJS client proxy for SQS producers
- `@EventPattern` decorator support following official NestJS patterns
- SqsContext for accessing message metadata
- OpenTelemetry observability support (optional)
- Full TypeScript support

[0.4.0]: https://github.com/amagdy46/nestjs-sqs-transporter/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/amagdy46/nestjs-sqs-transporter/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/amagdy46/nestjs-sqs-transporter/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/amagdy46/nestjs-sqs-transporter/releases/tag/v0.1.0
