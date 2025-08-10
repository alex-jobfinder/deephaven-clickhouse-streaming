"""
Test configuration and mock data for FeedHandler trades testing
"""

import time
from datetime import datetime, timezone

# Test data configurations
TEST_SYMBOLS = ['BTC-USD', 'ETH-USD', 'AVAX-USD', 'SOL-USD']
TEST_SYMBOLS_HYPERLIQUID = ["BTC", "ETH", "AVAX", "SOL"]

# Mock trade data for different exchanges
MOCK_TRADE_DATA = {
    'coinbase': {
        'symbol': 'BTC-USD',
        'exchange': 'COINBASE',
        'id': 'coinbase_trade_12345',
        'timestamp': 1640995200.0,
        'amount': 0.1,
        'price': 45000.0,
        'side': 'buy',
        'type': 'trade',
        'receipt_timestamp': 1640995200.001
    },
    'bitstamp': {
        'symbol': 'BTC-USD',
        'exchange': 'BITSTAMP',
        'id': 'bitstamp_trade_67890',
        'timestamp': 1640995201.0,
        'amount': 0.05,
        'price': 44999.0,
        'side': 'sell',
        'type': 'trade',
        'receipt_timestamp': 1640995201.001
    },
    'kraken': {
        'symbol': 'BTC-USD',
        'exchange': 'KRAKEN',
        'id': 'kraken_trade_11111',
        'timestamp': 1640995202.0,
        'amount': 0.2,
        'price': 45001.0,
        'side': 'buy',
        'type': 'trade',
        'receipt_timestamp': 1640995202.001
    },
    'hyperliquid': {
        'symbol': 'BTC',
        'exchange': 'HYPERLIQUID',
        'id': 'hyperliquid_trade_22222',
        'timestamp': 1640995203.0,
        'amount': 0.15,
        'price': 45000.5,
        'side': 'sell',
        'type': 'trade',
        'receipt_timestamp': 1640995203.001
    }
}

# Expected transformed data for ClickHouse
EXPECTED_TRANSFORMED_DATA = {
    'coinbase': {
        'symbol': 'BTC-USD',
        'exchange': 'COINBASE',
        'trade_id': 'coinbase_trade_12345',
        'ts': 1640995200000000000,  # nanoseconds
        'receipt_ts': 1640995200001000000,  # nanoseconds
        'size': 0.1,
        'price': 45000.0,
        'side': 'buy'
    },
    'bitstamp': {
        'symbol': 'BTC-USD',
        'exchange': 'BITSTAMP',
        'trade_id': 'bitstamp_trade_67890',
        'ts': 1640995201000000000,
        'receipt_ts': 1640995201001000000,
        'size': 0.05,
        'price': 44999.0,
        'side': 'sell'
    },
    'kraken': {
        'symbol': 'BTC-USD',
        'exchange': 'KRAKEN',
        'trade_id': 'kraken_trade_11111',
        'ts': 1640995202000000000,
        'receipt_ts': 1640995202001000000,
        'size': 0.2,
        'price': 45001.0,
        'side': 'buy'
    },
    'hyperliquid': {
        'symbol': 'BTC',
        'exchange': 'HYPERLIQUID',
        'trade_id': 'hyperliquid_trade_22222',
        'ts': 1640995203000000000,
        'receipt_ts': 1640995203001000000,
        'size': 0.15,
        'price': 45000.5,
        'side': 'sell'
    }
}

# Test scenarios
TEST_SCENARIOS = {
    'normal_trade': {
        'description': 'Normal trade processing',
        'input': MOCK_TRADE_DATA['coinbase'],
        'expected': EXPECTED_TRANSFORMED_DATA['coinbase']
    },
    'sell_trade': {
        'description': 'Sell trade processing',
        'input': MOCK_TRADE_DATA['bitstamp'],
        'expected': EXPECTED_TRANSFORMED_DATA['bitstamp']
    },
    'hyperliquid_trade': {
        'description': 'HyperLiquid trade processing',
        'input': MOCK_TRADE_DATA['hyperliquid'],
        'expected': EXPECTED_TRANSFORMED_DATA['hyperliquid']
    },
    'large_amount': {
        'description': 'Large amount trade',
        'input': {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': 'large_trade_1',
            'timestamp': 1640995200.0,
            'amount': 10.0,
            'price': 45000.0,
            'side': 'buy',
            'type': 'trade',
            'receipt_timestamp': 1640995200.001
        },
        'expected': {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'trade_id': 'large_trade_1',
            'ts': 1640995200000000000,
            'receipt_ts': 1640995200001000000,
            'size': 10.0,
            'price': 45000.0,
            'side': 'buy'
        }
    },
    'high_price': {
        'description': 'High price trade',
        'input': {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': 'high_price_trade_1',
            'timestamp': 1640995200.0,
            'amount': 0.01,
            'price': 100000.0,
            'side': 'sell',
            'type': 'trade',
            'receipt_timestamp': 1640995200.001
        },
        'expected': {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'trade_id': 'high_price_trade_1',
            'ts': 1640995200000000000,
            'receipt_ts': 1640995200001000000,
            'size': 0.01,
            'price': 100000.0,
            'side': 'sell'
        }
    }
}

# Error test scenarios
ERROR_SCENARIOS = {
    'missing_timestamp': {
        'description': 'Trade with missing timestamp',
        'input': {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': 'error_trade_1',
            'amount': 0.1,
            'price': 45000.0,
            'side': 'buy',
            'type': 'trade',
            'receipt_timestamp': 1640995200.001
        }
    },
    'missing_amount': {
        'description': 'Trade with missing amount',
        'input': {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': 'error_trade_2',
            'timestamp': 1640995200.0,
            'price': 45000.0,
            'side': 'buy',
            'type': 'trade',
            'receipt_timestamp': 1640995200.001
        }
    },
    'missing_price': {
        'description': 'Trade with missing price',
        'input': {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': 'error_trade_3',
            'timestamp': 1640995200.0,
            'amount': 0.1,
            'side': 'buy',
            'type': 'trade',
            'receipt_timestamp': 1640995200.001
        }
    },
    'invalid_side': {
        'description': 'Trade with invalid side',
        'input': {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': 'error_trade_4',
            'timestamp': 1640995200.0,
            'amount': 0.1,
            'price': 45000.0,
            'side': 'invalid',
            'type': 'trade',
            'receipt_timestamp': 1640995200.001
        }
    }
}

# Performance test configurations
PERFORMANCE_CONFIG = {
    'small_batch': {
        'num_trades': 10,
        'description': 'Small batch of trades'
    },
    'medium_batch': {
        'num_trades': 100,
        'description': 'Medium batch of trades'
    },
    'large_batch': {
        'num_trades': 1000,
        'description': 'Large batch of trades'
    }
}

# Kafka configuration for testing
TEST_KAFKA_CONFIG = {
    'local': {
        'bootstrap': 'localhost',
        'port': 9092
    },
    'docker': {
        'bootstrap': 'redpanda',
        'port': 29092
    }
}

# Exchange configurations for testing
TEST_EXCHANGE_CONFIG = {
    'coinbase': {
        'name': 'COINBASE',
        'symbols': TEST_SYMBOLS,
        'channels': ['trades']
    },
    'bitstamp': {
        'name': 'BITSTAMP',
        'symbols': TEST_SYMBOLS,
        'channels': ['trades']
    },
    'kraken': {
        'name': 'KRAKEN',
        'symbols': TEST_SYMBOLS,
        'channels': ['trades']
    },
    'hyperliquid': {
        'name': 'HYPERLIQUID',
        'symbols': TEST_SYMBOLS_HYPERLIQUID,
        'channels': ['trades']
    }
}

def generate_test_trades(num_trades=10, exchange='COINBASE', symbol='BTC-USD'):
    """Generate a list of test trades"""
    trades = []
    base_timestamp = time.time()
    
    for i in range(num_trades):
        trade = {
            'symbol': symbol,
            'exchange': exchange,
            'id': f'{exchange.lower()}_trade_{i}',
            'timestamp': base_timestamp + i,
            'amount': 0.1 + (i * 0.01),
            'price': 45000.0 + (i * 10),
            'side': 'buy' if i % 2 == 0 else 'sell',
            'type': 'trade',
            'receipt_timestamp': base_timestamp + i + 0.001
        }
        trades.append(trade)
    
    return trades

def get_expected_transformed_trade(trade_data):
    """Get expected transformed trade data"""
    return {
        'symbol': trade_data['symbol'],
        'exchange': trade_data['exchange'],
        'trade_id': trade_data['id'],
        'ts': int(trade_data['timestamp'] * 1_000_000_000),
        'receipt_ts': int(trade_data['receipt_timestamp'] * 1_000_000_000),
        'size': trade_data['amount'],
        'price': trade_data['price'],
        'side': trade_data['side']
    }
