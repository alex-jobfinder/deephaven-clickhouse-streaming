import unittest
import asyncio
import json
import time
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import sys
import os

# Add the src directory to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from cryptofeed.exchanges import Coinbase, Bitstamp, Kraken
from cryptofeed.types import Trade

import cryptofeed_tools as cft


class TestIntegrationTrades(unittest.TestCase):
    """Integration tests for the complete trades data flow"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        self.mock_kafka_data = []
        self.mock_print_output = []

    def mock_kafka_send(self, topic, data):
        """Mock Kafka send function to capture data"""
        self.mock_kafka_data.append({
            'topic': topic,
            'data': json.loads(data.decode('utf-8'))
        })

    def mock_print(self, data, receipt_time):
        """Mock print function to capture output"""
        self.mock_print_output.append({
            'data': data,
            'receipt_time': receipt_time
        })

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_complete_trade_flow(self, mock_producer_class):
        """Test complete flow from trade data to Kafka"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        # Create callback instances
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Simulate trade data from exchange
        trade_data = {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': 'trade_12345',
            'timestamp': time.time(),
            'amount': 0.5,
            'price': 45000.0,
            'side': 'buy',
            'type': 'trade',
            'receipt_timestamp': time.time() + 0.001
        }
        
        # Process the trade data
        asyncio.run(kafka_callback.write(trade_data))
        
        # Verify data was sent to Kafka
        self.assertEqual(len(self.mock_kafka_data), 1)
        sent_data = self.mock_kafka_data[0]
        
        self.assertEqual(sent_data['topic'], 'trades')
        self.assertIn('ts', sent_data['data'])
        self.assertIn('receipt_ts', sent_data['data'])
        self.assertIn('size', sent_data['data'])
        self.assertIn('trade_id', sent_data['data'])
        self.assertIn('symbol', sent_data['data'])
        self.assertIn('exchange', sent_data['data'])
        self.assertIn('price', sent_data['data'])
        self.assertIn('side', sent_data['data'])

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_multiple_trades_processing(self, mock_producer_class):
        """Test processing multiple trades in sequence"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Create multiple trade data entries
        trades = [
            {
                'symbol': 'BTC-USD',
                'exchange': 'COINBASE',
                'id': 'trade_1',
                'timestamp': time.time(),
                'amount': 0.1,
                'price': 45000.0,
                'side': 'buy',
                'type': 'trade',
                'receipt_timestamp': time.time() + 0.001
            },
            {
                'symbol': 'ETH-USD',
                'exchange': 'BITSTAMP',
                'id': 'trade_2',
                'timestamp': time.time() + 1,
                'amount': 2.0,
                'price': 3000.0,
                'side': 'sell',
                'type': 'trade',
                'receipt_timestamp': time.time() + 1.001
            },
            {
                'symbol': 'AVAX-USD',
                'exchange': 'KRAKEN',
                'id': 'trade_3',
                'timestamp': time.time() + 2,
                'amount': 10.0,
                'price': 25.0,
                'side': 'buy',
                'type': 'trade',
                'receipt_timestamp': time.time() + 2.001
            }
        ]
        
        # Process all trades
        for trade in trades:
            asyncio.run(kafka_callback.write(trade))
        
        # Verify all trades were processed
        self.assertEqual(len(self.mock_kafka_data), 3)
        
        # Verify each trade was processed correctly
        for i, trade in enumerate(trades):
            sent_data = self.mock_kafka_data[i]['data']
            self.assertEqual(sent_data['symbol'], trade['symbol'])
            self.assertEqual(sent_data['exchange'], trade['exchange'])
            self.assertEqual(sent_data['trade_id'], trade['id'])
            self.assertEqual(sent_data['size'], trade['amount'])
            self.assertEqual(sent_data['price'], trade['price'])
            self.assertEqual(sent_data['side'], trade['side'])

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_different_exchanges_data_format(self, mock_producer_class):
        """Test handling data from different exchanges with varying formats"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Test data from different exchanges
        exchange_trades = {
            'COINBASE': {
                'symbol': 'BTC-USD',
                'exchange': 'COINBASE',
                'id': 'coinbase_trade_1',
                'timestamp': time.time(),
                'amount': 0.1,
                'price': 45000.0,
                'side': 'buy',
                'type': 'trade',
                'receipt_timestamp': time.time() + 0.001
            },
            'BITSTAMP': {
                'symbol': 'BTC-USD',
                'exchange': 'BITSTAMP',
                'id': 'bitstamp_trade_1',
                'timestamp': time.time() + 1,
                'amount': 0.05,
                'price': 44999.0,
                'side': 'sell',
                'type': 'trade',
                'receipt_timestamp': time.time() + 1.001
            },
            'KRAKEN': {
                'symbol': 'BTC-USD',
                'exchange': 'KRAKEN',
                'id': 'kraken_trade_1',
                'timestamp': time.time() + 2,
                'amount': 0.2,
                'price': 45001.0,
                'side': 'buy',
                'type': 'trade',
                'receipt_timestamp': time.time() + 2.001
            }
        }
        
        # Process trades from all exchanges
        for exchange, trade in exchange_trades.items():
            asyncio.run(kafka_callback.write(trade))
        
        # Verify all exchanges were processed
        self.assertEqual(len(self.mock_kafka_data), 3)
        
        # Verify exchange-specific data
        for i, (exchange, trade) in enumerate(exchange_trades.items()):
            sent_data = self.mock_kafka_data[i]['data']
            self.assertEqual(sent_data['exchange'], exchange)
            self.assertEqual(sent_data['trade_id'], trade['id'])

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_hyperliquid_trades_processing(self, mock_producer_class):
        """Test processing HyperLiquid trades with different symbol format"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # HyperLiquid uses different symbol format (no hyphen)
        hyperliquid_trades = [
            {
                'symbol': 'BTC',
                'exchange': 'HYPERLIQUID',
                'id': 'hl_trade_1',
                'timestamp': time.time(),
                'amount': 0.1,
                'price': 45000.0,
                'side': 'buy',
                'type': 'trade',
                'receipt_timestamp': time.time() + 0.001
            },
            {
                'symbol': 'ETH',
                'exchange': 'HYPERLIQUID',
                'id': 'hl_trade_2',
                'timestamp': time.time() + 1,
                'amount': 1.0,
                'price': 3000.0,
                'side': 'sell',
                'type': 'trade',
                'receipt_timestamp': time.time() + 1.001
            }
        ]
        
        # Process HyperLiquid trades
        for trade in hyperliquid_trades:
            asyncio.run(kafka_callback.write(trade))
        
        # Verify HyperLiquid trades were processed
        self.assertEqual(len(self.mock_kafka_data), 2)
        
        for i, trade in enumerate(hyperliquid_trades):
            sent_data = self.mock_kafka_data[i]['data']
            self.assertEqual(sent_data['symbol'], trade['symbol'])
            self.assertEqual(sent_data['exchange'], 'HYPERLIQUID')

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_trade_data_validation(self, mock_producer_class):
        """Test validation of trade data fields"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Test valid trade data
        valid_trade = {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': 'valid_trade_1',
            'timestamp': time.time(),
            'amount': 0.1,
            'price': 45000.0,
            'side': 'buy',
            'type': 'trade',
            'receipt_timestamp': time.time() + 0.001
        }
        
        asyncio.run(kafka_callback.write(valid_trade))
        
        # Verify valid trade was processed
        self.assertEqual(len(self.mock_kafka_data), 1)
        sent_data = self.mock_kafka_data[0]['data']
        
        # Verify all required fields are present
        required_fields = ['symbol', 'exchange', 'trade_id', 'ts', 'receipt_ts', 'size', 'price', 'side']
        for field in required_fields:
            self.assertIn(field, sent_data)

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_timestamp_conversion_accuracy(self, mock_producer_class):
        """Test accuracy of timestamp conversion to nanoseconds"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Test with specific timestamps
        test_timestamp = 1640995200.0  # 2022-01-01 00:00:00 UTC
        test_receipt_timestamp = 1640995201.0  # 2022-01-01 00:00:01 UTC
        
        trade_data = {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': 'timestamp_test_1',
            'timestamp': test_timestamp,
            'amount': 0.1,
            'price': 45000.0,
            'side': 'buy',
            'type': 'trade',
            'receipt_timestamp': test_receipt_timestamp
        }
        
        asyncio.run(kafka_callback.write(trade_data))
        
        # Verify timestamp conversion
        sent_data = self.mock_kafka_data[0]['data']
        expected_ts = int(test_timestamp * 1_000_000_000)
        expected_receipt_ts = int(test_receipt_timestamp * 1_000_000_000)
        
        self.assertEqual(sent_data['ts'], expected_ts)
        self.assertEqual(sent_data['receipt_ts'], expected_receipt_ts)

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_concurrent_trade_processing(self, mock_producer_class):
        """Test processing multiple trades concurrently"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Create multiple trades to process concurrently
        trades = []
        for i in range(10):
            trades.append({
                'symbol': f'BTC-USD',
                'exchange': 'COINBASE',
                'id': f'concurrent_trade_{i}',
                'timestamp': time.time() + i,
                'amount': 0.1 + (i * 0.01),
                'price': 45000.0 + (i * 10),
                'side': 'buy' if i % 2 == 0 else 'sell',
                'type': 'trade',
                'receipt_timestamp': time.time() + i + 0.001
            })
        
        # Process trades concurrently
        async def process_trades():
            tasks = []
            for trade in trades:
                task = kafka_callback.write(trade)
                tasks.append(task)
            await asyncio.gather(*tasks)
        
        asyncio.run(process_trades())
        
        # Verify all trades were processed
        self.assertEqual(len(self.mock_kafka_data), 10)
        
        # Verify each trade was processed correctly
        for i, trade in enumerate(trades):
            sent_data = self.mock_kafka_data[i]['data']
            self.assertEqual(sent_data['trade_id'], trade['id'])
            self.assertEqual(sent_data['size'], trade['amount'])
            self.assertEqual(sent_data['price'], trade['price'])
            self.assertEqual(sent_data['side'], trade['side'])

    def test_symbol_list_validation(self):
        """Test that all symbols in the configuration are valid"""
        # Test standard symbols
        for symbol in cft.SYMBOLS:
            self.assertIsInstance(symbol, str)
            self.assertGreater(len(symbol), 0)
            self.assertIn('-', symbol)  # Should contain hyphen for standard exchanges
        
        # Test HyperLiquid symbols
        for symbol in cft.SYMBOLS_HYPERLIQUID:
            self.assertIsInstance(symbol, str)
            self.assertGreater(len(symbol), 0)
            self.assertNotIn('-', symbol)  # Should not contain hyphen for HyperLiquid

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_error_recovery(self, mock_producer_class):
        """Test that the system can recover from errors"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Test with valid trade after error
        valid_trade = {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': 'recovery_test_1',
            'timestamp': time.time(),
            'amount': 0.1,
            'price': 45000.0,
            'side': 'buy',
            'type': 'trade',
            'receipt_timestamp': time.time() + 0.001
        }
        
        # Process valid trade
        asyncio.run(kafka_callback.write(valid_trade))
        
        # Verify trade was processed despite previous errors
        self.assertEqual(len(self.mock_kafka_data), 1)
        sent_data = self.mock_kafka_data[0]['data']
        self.assertEqual(sent_data['trade_id'], 'recovery_test_1')


if __name__ == '__main__':
    unittest.main()
