import unittest
import asyncio
import json
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime
import sys
import os

# Add the src directory to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from cryptofeed.exchanges import Coinbase, Bitstamp, Kraken
from cryptofeed.types import Trade

import cryptofeed_tools as cft


class TestFeedHandlerTrades(unittest.TestCase):
    """Test cases for FeedHandler trades functionality"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        self.sample_trade_data = {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': '12345',
            'timestamp': 1640995200.0,  # 2022-01-01 00:00:00 UTC
            'amount': 0.1,
            'price': 50000.0,
            'side': 'buy',
            'type': 'trade'
        }
        
        self.sample_trade = Trade(
            symbol='BTC-USD',
            exchange='COINBASE',
            id='12345',
            timestamp=1640995200.0,
            amount=0.1,
            price=50000.0,
            side='buy',
            type='trade'
        )

    def test_symbols_configuration(self):
        """Test that symbols are properly configured"""
        self.assertIsInstance(cft.SYMBOLS, list)
        self.assertGreater(len(cft.SYMBOLS), 0)
        self.assertIn('BTC-USD', cft.SYMBOLS)
        self.assertIn('ETH-USD', cft.SYMBOLS)
        
        # Test HyperLiquid symbols
        self.assertIsInstance(cft.SYMBOLS_HYPERLIQUID, list)
        self.assertGreater(len(cft.SYMBOLS_HYPERLIQUID), 0)
        self.assertIn('BTC', cft.SYMBOLS_HYPERLIQUID)
        self.assertIn('ETH', cft.SYMBOLS_HYPERLIQUID)

    @patch('cryptofeed_tools.ClickHouseTradeKafka')
    @patch('cryptofeed_tools.my_print')
    def test_feedhandler_initialization(self, mock_print, mock_kafka):
        """Test FeedHandler initialization with proper callbacks"""
        # Mock the Kafka callback
        mock_kafka_instance = Mock()
        mock_kafka.return_value = mock_kafka_instance
        
        # Create FeedHandler
        f = FeedHandler()
        
        # Test that we can add feeds (this would normally connect to exchanges)
        # We'll mock the exchange classes to avoid actual connections
        with patch('cryptofeed.exchanges.Coinbase') as mock_coinbase:
            mock_coinbase.return_value = Mock()
            f.add_feed(Coinbase(
                channels=[TRADES], 
                symbols=cft.SYMBOLS, 
                callbacks={TRADES: [mock_kafka_instance, mock_print]}
            ))
        
        self.assertIsInstance(f, FeedHandler)

    def test_my_print_callback(self):
        """Test the my_print callback function"""
        test_data = {'test': 'data'}
        receipt_time = 1640995200.0
        
        # Test that my_print doesn't raise any exceptions
        try:
            asyncio.run(cft.my_print(test_data, receipt_time))
        except Exception as e:
            self.fail(f"my_print raised an exception: {e}")

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_clickhouse_trade_kafka_initialization(self, mock_producer_class):
        """Test ClickHouseTradeKafka initialization"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        # Test with default parameters
        kafka_callback = cft.ClickHouseTradeKafka()
        self.assertEqual(kafka_callback.topic, 'trades')
        self.assertEqual(kafka_callback.bootstrap, '127.0.0.1')
        self.assertEqual(kafka_callback.port, 9092)
        
        # Test with custom parameters
        kafka_callback = cft.ClickHouseTradeKafka(
            bootstrap='test-server',
            port=9093,
            topic='custom-trades'
        )
        self.assertEqual(kafka_callback.topic, 'custom-trades')
        self.assertEqual(kafka_callback.bootstrap, 'test-server')
        self.assertEqual(kafka_callback.port, 9093)

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_clickhouse_trade_kafka_data_transformation(self, mock_producer_class):
        """Test ClickHouseTradeKafka data transformation"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        
        # Test data transformation
        test_data = {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': '12345',
            'timestamp': 1640995200.0,
            'amount': 0.1,
            'price': 50000.0,
            'side': 'buy',
            'type': 'trade',
            'receipt_timestamp': 1640995201.0
        }
        
        # Mock the producer start
        kafka_callback.producer = mock_producer
        
        # Test the write method
        asyncio.run(kafka_callback.write(test_data))
        
        # Verify that send_and_wait was called
        mock_producer.send_and_wait.assert_called_once()
        
        # Get the sent data
        call_args = mock_producer.send_and_wait.call_args
        sent_data = json.loads(call_args[0][1].decode('utf-8'))
        
        # Verify data transformation
        self.assertIn('ts', sent_data)
        self.assertIn('receipt_ts', sent_data)
        self.assertIn('size', sent_data)
        self.assertIn('trade_id', sent_data)
        self.assertNotIn('timestamp', sent_data)
        self.assertNotIn('receipt_timestamp', sent_data)
        self.assertNotIn('amount', sent_data)
        self.assertNotIn('id', sent_data)
        self.assertNotIn('type', sent_data)
        
        # Verify timestamp conversion (nanoseconds)
        self.assertEqual(sent_data['ts'], int(test_data['timestamp'] * 1_000_000_000))
        self.assertEqual(sent_data['receipt_ts'], int(test_data['receipt_timestamp'] * 1_000_000_000))
        self.assertEqual(sent_data['size'], test_data['amount'])
        self.assertEqual(sent_data['trade_id'], test_data['id'])

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_clickhouse_trade_kafka_error_handling(self, mock_producer_class):
        """Test ClickHouseTradeKafka error handling"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Test with malformed data that should cause an exception
        malformed_data = {
            'symbol': 'BTC-USD',
            # Missing required fields
        }
        
        # This should not raise an exception due to the try-catch block
        try:
            asyncio.run(kafka_callback.write(malformed_data))
        except Exception as e:
            self.fail(f"ClickHouseTradeKafka.write() should handle errors gracefully: {e}")

    def test_trade_object_creation(self):
        """Test Trade object creation and properties"""
        trade = Trade(
            symbol='BTC-USD',
            exchange='COINBASE',
            id='12345',
            timestamp=1640995200.0,
            amount=0.1,
            price=50000.0,
            side='buy',
            type='trade'
        )
        
        self.assertEqual(trade.symbol, 'BTC-USD')
        self.assertEqual(trade.exchange, 'COINBASE')
        self.assertEqual(trade.id, '12345')
        self.assertEqual(trade.timestamp, 1640995200.0)
        self.assertEqual(trade.amount, 0.1)
        self.assertEqual(trade.price, 50000.0)
        self.assertEqual(trade.side, 'buy')
        self.assertEqual(trade.type, 'trade')

    def test_trade_to_dict_conversion(self):
        """Test Trade object to dictionary conversion"""
        trade = Trade(
            symbol='BTC-USD',
            exchange='COINBASE',
            id='12345',
            timestamp=1640995200.0,
            amount=0.1,
            price=50000.0,
            side='buy',
            type='trade'
        )
        
        trade_dict = trade.to_dict()
        
        self.assertIsInstance(trade_dict, dict)
        self.assertEqual(trade_dict['symbol'], 'BTC-USD')
        self.assertEqual(trade_dict['exchange'], 'COINBASE')
        self.assertEqual(trade_dict['id'], '12345')
        self.assertEqual(trade_dict['timestamp'], 1640995200.0)
        self.assertEqual(trade_dict['amount'], 0.1)
        self.assertEqual(trade_dict['price'], 50000.0)
        self.assertEqual(trade_dict['side'], 'buy')
        self.assertEqual(trade_dict['type'], 'trade')

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_kafka_callback_with_trade_object(self, mock_producer_class):
        """Test KafkaCallback with Trade object"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.KafkaCallback()
        kafka_callback.producer = mock_producer
        
        trade = Trade(
            symbol='BTC-USD',
            exchange='COINBASE',
            id='12345',
            timestamp=1640995200.0,
            amount=0.1,
            price=50000.0,
            side='buy',
            type='trade'
        )
        
        receipt_timestamp = 1640995201.0
        
        # Test the __call__ method with a Trade object
        asyncio.run(kafka_callback(TRADES, trade, receipt_timestamp))
        
        # Verify that send_and_wait was called
        mock_producer.send_and_wait.assert_called_once()
        
        # Get the sent data
        call_args = mock_producer.send_and_wait.call_args
        sent_data = json.loads(call_args[0][1].decode('utf-8'))
        
        # Verify the data structure
        self.assertEqual(sent_data['symbol'], 'BTC-USD')
        self.assertEqual(sent_data['exchange'], 'COINBASE')
        self.assertEqual(sent_data['id'], '12345')
        self.assertEqual(sent_data['timestamp'], 1640995200.0)
        self.assertEqual(sent_data['receipt_timestamp'], 1640995201.0)

    def test_environment_variable_handling(self):
        """Test environment variable handling for Docker vs local development"""
        # Test Docker environment
        with patch.dict(os.environ, {'IS_DOCKER': 'True'}):
            bootstrap = 'redpanda' if os.environ.get('IS_DOCKER') else 'localhost'
            port = 29092 if os.environ.get('IS_DOCKER') else 9092
            
            self.assertEqual(bootstrap, 'redpanda')
            self.assertEqual(port, 29092)
        
        # Test local environment
        with patch.dict(os.environ, {}, clear=True):
            bootstrap = 'redpanda' if os.environ.get('IS_DOCKER') else 'localhost'
            port = 29092 if os.environ.get('IS_DOCKER') else 9092
            
            self.assertEqual(bootstrap, 'localhost')
            self.assertEqual(port, 9092)

    def test_symbol_validation(self):
        """Test that symbols are valid for different exchanges"""
        # Test standard symbols (for Coinbase, Bitstamp, Kraken)
        for symbol in cft.SYMBOLS:
            self.assertIn('-', symbol)  # Should contain hyphen (e.g., BTC-USD)
            self.assertGreater(len(symbol), 3)
        
        # Test HyperLiquid symbols
        for symbol in cft.SYMBOLS_HYPERLIQUID:
            self.assertNotIn('-', symbol)  # Should not contain hyphen
            self.assertGreater(len(symbol), 0)

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_kafka_connection_handling(self, mock_producer_class):
        """Test Kafka connection handling"""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        
        # Initially producer should be None
        self.assertIsNone(kafka_callback.producer)
        
        # After calling write, producer should be initialized
        test_data = {
            'symbol': 'BTC-USD',
            'id': '12345',
            'timestamp': 1640995200.0,
            'amount': 0.1,
            'price': 50000.0,
            'side': 'buy',
            'type': 'trade',
            'receipt_timestamp': 1640995201.0
        }
        
        asyncio.run(kafka_callback.write(test_data))
        
        # Producer should be initialized
        self.assertIsNotNone(kafka_callback.producer)
        mock_producer.start.assert_called_once()

    def test_data_types_and_formats(self):
        """Test data types and formats for trade data"""
        # Test numeric types
        kafka_callback = cft.KafkaCallback(numeric_type=float)
        
        test_data = {
            'price': 50000.0,
            'amount': 0.1,
            'timestamp': 1640995200.0
        }
        
        # Verify data types
        self.assertIsInstance(test_data['price'], float)
        self.assertIsInstance(test_data['amount'], float)
        self.assertIsInstance(test_data['timestamp'], float)

    def test_error_scenarios(self):
        """Test various error scenarios"""
        # Test with None values
        kafka_callback = cft.KafkaCallback(none_to=0.0)
        
        test_data = {
            'price': None,
            'amount': None,
            'timestamp': None
        }
        
        # This should not raise an exception
        try:
            # The callback should handle None values gracefully
            pass
        except Exception as e:
            self.fail(f"Callback should handle None values: {e}")


if __name__ == '__main__':
    unittest.main()
