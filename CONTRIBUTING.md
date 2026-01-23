# Contributing to nestjs-sqs-transporter

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Node.js >= 18.0.0
- npm

### Getting Started

1. Fork and clone the repository:
   ```bash
   git clone https://github.com/YOUR_USERNAME/nestjs-sqs-transporter.git
   cd nestjs-sqs-transporter
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Run tests to verify setup:
   ```bash
   npm test
   ```

## Development Workflow

### Running Tests

```bash
# Run all tests
npm test

# Run tests in watch mode
npm run test:watch

# Run tests with coverage
npm run test:coverage
```

### Code Style

This project uses [Biome](https://biomejs.dev/) for linting and formatting:

```bash
# Check for linting issues
npm run lint

# Fix linting issues automatically
npm run lint:fix

# Format code
npm run format
```

### Building

```bash
# Build the project
npm run build

# Build in watch mode
npm run dev
```

## Pull Request Process

1. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following the code style guidelines

3. **Write or update tests** for your changes

4. **Ensure all tests pass**:
   ```bash
   npm test
   ```

5. **Ensure linting passes**:
   ```bash
   npm run lint
   ```

6. **Commit your changes** with a clear commit message:
   ```bash
   git commit -m "feat: add new feature description"
   ```

7. **Push to your fork** and create a Pull Request

### Commit Message Format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `test:` - Adding or updating tests
- `refactor:` - Code refactoring
- `chore:` - Maintenance tasks

## Reporting Issues

When reporting issues, please include:

1. A clear description of the problem
2. Steps to reproduce
3. Expected vs actual behavior
4. Node.js and package versions
5. Relevant code snippets or error messages

## Code of Conduct

Please be respectful and constructive in all interactions. We're all here to build something great together.

## Questions?

If you have questions, feel free to open an issue or reach out through GitHub discussions.
