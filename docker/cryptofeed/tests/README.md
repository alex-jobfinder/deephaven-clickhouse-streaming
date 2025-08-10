# FeedHandler Trades Unit Tests

This directory contains comprehensive unit tests for the FeedHandler trades functionality in the cryptofeed trading system.

## Overview

The tests cover the complete data flow from cryptocurrency exchanges through the FeedHandler to Kafka and ClickHouse, including:

- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end data flow testing
- **Performance Tests**: Throughput and latency measurements
- **Error Handling Tests**: Robustness and error recovery

## Test Structure

```
tests/
├── __init__.py                    # Package initialization
├── test_feedhandler_trades.py     # Main unit tests
├── test_integration_trades.py     # Integration tests
├── test_performance_trades.py     # Performance tests
├── test_config.py                 # Test configuration and mock data
├── run_tests.py                   # Test runner script
├── requirements-test.txt          # Test dependencies
└── README.md                     # This file
```

## Test Categories

### 1. Unit Tests (`test_feedhandler_trades.py`)

Tests individual components and functions:

- **Symbol Configuration**: Validates symbol lists for different exchanges
- **FeedHandler Initialization**: Tests proper setup and configuration
- **Callback Functions**: Tests `my_print` and Kafka callbacks
- **Data Transformation**: Tests ClickHouse data format conversion
- **Error Handling**: Tests graceful error recovery
- **Environment Variables**: Tests Docker vs local configuration
- **Data Types**: Tests numeric type handling and validation

### 2. Integration Tests (`test_integration_trades.py`)

Tests complete data flow scenarios:

- **Complete Trade Flow**: End-to-end trade processing
- **Multiple Trades**: Sequential and concurrent processing
- **Different Exchanges**: Coinbase, Bitstamp, Kraken, HyperLiquid
- **Data Validation**: Field validation and transformation
- **Timestamp Conversion**: Nanosecond precision testing
- **Concurrent Processing**: Async trade processing
- **Error Recovery**: System recovery from failures

### 3. Performance Tests (`test_performance_trades.py`)

Measures system performance:

- **Single Trade Latency**: Individual trade processing time
- **Batch Throughput**: Multiple trades processing rate
- **Concurrent Performance**: Async processing efficiency
- **Memory Usage**: Memory consumption under load
- **Latency Distribution**: Statistical analysis of processing times
- **Error Handling Performance**: Performance with mixed valid/invalid data

## Running Tests

### Prerequisites

1. Install test dependencies:
```bash
pip install -r requirements-test.txt
```

2. Ensure the main cryptofeed dependencies are installed:
```bash
pip install -r ../requirements.txt
```

### Basic Test Execution

Run all tests:
```bash
python run_tests.py --all
```

Run specific test types:
```bash
# Unit tests only
python run_tests.py --unit

# Integration tests only
python run_tests.py --integration

# Performance tests only
python run_tests.py --performance
```

### Advanced Options

Verbose output:
```bash
python run_tests.py --all --verbose
```

Generate detailed report:
```bash
python run_tests.py --all --report
```

Run specific test pattern:
```bash
python run_tests.py --pattern "test_*kafka*"
```

Run specific test:
```bash
python run_tests.py --specific "TestFeedHandlerTrades.test_symbols_configuration"
```

### Using pytest (Alternative)

Run with pytest for more features:
```bash
# Install pytest
pip install pytest pytest-asyncio pytest-cov

# Run all tests
pytest

# Run with coverage
pytest --cov=../src

# Run specific test file
pytest test_feedhandler_trades.py

# Run with verbose output
pytest -v
```

## Test Configuration

The `test_config.py` file contains:

- **Mock Trade Data**: Sample trade data for different exchanges
- **Expected Transformations**: Expected ClickHouse data format
- **Test Scenarios**: Various test cases and edge cases
- **Error Scenarios**: Invalid data for error testing
- **Performance Configs**: Batch sizes and performance thresholds

## Key Test Scenarios

### 1. Normal Trade Processing
```python
# Tests standard trade data flow
trade_data = {
    'symbol': 'BTC-USD',
    'exchange': 'COINBASE',
    'id': 'trade_12345',
    'timestamp': 1640995200.0,
    'amount': 0.1,
    'price': 45000.0,
    'side': 'buy',
    'type': 'trade',
    'receipt_timestamp': 1640995200.001
}
```

### 2. Data Transformation
Tests the conversion from cryptofeed format to ClickHouse format:
- `timestamp` → `ts` (nanoseconds)
- `receipt_timestamp` → `receipt_ts` (nanoseconds)
- `amount` → `size`
- `id` → `trade_id`
- Removes `type` field

### 3. Error Handling
Tests graceful handling of:
- Missing required fields
- Invalid data types
- Network errors
- Kafka connection issues

### 4. Performance Benchmarks
- **Latency**: < 100ms for single trade
- **Throughput**: > 10 trades/second
- **Memory**: < 100MB increase under load
- **Concurrent**: > 20 trades/second

## Mocking Strategy

The tests use comprehensive mocking to avoid:
- Real network connections to exchanges
- Actual Kafka/ClickHouse connections
- External dependencies

Key mocks:
- `AIOKafkaProducer`: Mocks Kafka producer
- Exchange classes: Mocks exchange connections
- Network calls: Mocks HTTP/WebSocket connections

## Test Data

### Symbol Lists
- **Standard Exchanges**: `['BTC-USD', 'ETH-USD', 'AVAX-USD', 'SOL-USD']`
- **HyperLiquid**: `['BTC', 'ETH', 'AVAX', 'SOL']`

### Exchange Configurations
- **Coinbase**: Standard REST API
- **Bitstamp**: WebSocket API
- **Kraken**: WebSocket API
- **HyperLiquid**: Custom WebSocket API

## Continuous Integration

The tests are designed to run in CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run Tests
  run: |
    cd docker/cryptofeed/tests
    python run_tests.py --all --report
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure `src` directory is in Python path
2. **Mock Issues**: Check mock setup and side effects
3. **Async Errors**: Verify asyncio event loop handling
4. **Performance Failures**: Adjust thresholds based on system capabilities

### Debug Mode

Run tests with debug output:
```bash
python run_tests.py --all --verbose --report
```

### Coverage Analysis

Generate coverage report:
```bash
coverage run -m pytest
coverage report
coverage html  # Generate HTML report
```

## Contributing

When adding new tests:

1. Follow the existing test structure
2. Use descriptive test names
3. Include both positive and negative test cases
4. Add performance benchmarks for new features
5. Update this README with new test categories

## Test Maintenance

- Update mock data when exchange APIs change
- Adjust performance thresholds based on system improvements
- Add new test cases for new exchange integrations
- Review and update error scenarios regularly
