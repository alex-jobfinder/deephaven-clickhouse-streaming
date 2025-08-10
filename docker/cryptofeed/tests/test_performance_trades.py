import unittest
import asyncio
import time
import json
import statistics
from unittest.mock import Mock, patch, AsyncMock
import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import cryptofeed_tools as cft
from test_config import generate_test_trades, PERFORMANCE_CONFIG


class TestPerformanceTrades(unittest.TestCase):
    """Performance tests for trade processing"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        self.mock_kafka_data = []
        self.processing_times = []

    def mock_kafka_send(self, topic, data):
        """Mock Kafka send function to capture data and timing"""
        self.mock_kafka_data.append({
            'topic': topic,
            'data': json.loads(data.decode('utf-8')),
            'timestamp': time.time()
        })

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_single_trade_processing_latency(self, mock_producer_class):
        """Test latency of processing a single trade"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Test single trade processing time
        trade_data = {
            'symbol': 'BTC-USD',
            'exchange': 'COINBASE',
            'id': 'perf_test_1',
            'timestamp': time.time(),
            'amount': 0.1,
            'price': 45000.0,
            'side': 'buy',
            'type': 'trade',
            'receipt_timestamp': time.time() + 0.001
        }
        
        start_time = time.time()
        asyncio.run(kafka_callback.write(trade_data))
        end_time = time.time()
        
        processing_time = (end_time - start_time) * 1000  # Convert to milliseconds
        self.processing_times.append(processing_time)
        
        # Verify trade was processed
        self.assertEqual(len(self.mock_kafka_data), 1)
        self.assertLess(processing_time, 100)  # Should be less than 100ms

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_batch_trade_processing_throughput(self, mock_producer_class):
        """Test throughput of processing multiple trades"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Generate test trades
        num_trades = 100
        trades = generate_test_trades(num_trades, 'COINBASE', 'BTC-USD')
        
        start_time = time.time()
        
        # Process trades sequentially
        for trade in trades:
            asyncio.run(kafka_callback.write(trade))
        
        end_time = time.time()
        
        total_time = end_time - start_time
        throughput = num_trades / total_time  # trades per second
        
        # Verify all trades were processed
        self.assertEqual(len(self.mock_kafka_data), num_trades)
        
        # Performance assertions
        self.assertGreater(throughput, 10)  # Should process at least 10 trades per second
        self.assertLess(total_time, 10)  # Should complete in less than 10 seconds

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_concurrent_trade_processing_performance(self, mock_producer_class):
        """Test performance of concurrent trade processing"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Generate test trades
        num_trades = 50
        trades = generate_test_trades(num_trades, 'COINBASE', 'BTC-USD')
        
        start_time = time.time()
        
        # Process trades concurrently
        async def process_trades():
            tasks = []
            for trade in trades:
                task = kafka_callback.write(trade)
                tasks.append(task)
            await asyncio.gather(*tasks)
        
        asyncio.run(process_trades())
        
        end_time = time.time()
        
        total_time = end_time - start_time
        throughput = num_trades / total_time  # trades per second
        
        # Verify all trades were processed
        self.assertEqual(len(self.mock_kafka_data), num_trades)
        
        # Performance assertions
        self.assertGreater(throughput, 20)  # Should process at least 20 trades per second concurrently
        self.assertLess(total_time, 5)  # Should complete in less than 5 seconds

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_memory_usage_under_load(self, mock_producer_class):
        """Test memory usage when processing many trades"""
        import psutil
        import os
        
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Generate and process many trades
        num_trades = 1000
        trades = generate_test_trades(num_trades, 'COINBASE', 'BTC-USD')
        
        for trade in trades:
            asyncio.run(kafka_callback.write(trade))
        
        # Get final memory usage
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Verify all trades were processed
        self.assertEqual(len(self.mock_kafka_data), num_trades)
        
        # Memory usage should be reasonable (less than 100MB increase)
        self.assertLess(memory_increase, 100)

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_different_batch_sizes_performance(self, mock_producer_class):
        """Test performance with different batch sizes"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        batch_sizes = [10, 50, 100, 500]
        performance_results = {}
        
        for batch_size in batch_sizes:
            self.mock_kafka_data.clear()  # Clear previous data
            
            trades = generate_test_trades(batch_size, 'COINBASE', 'BTC-USD')
            
            start_time = time.time()
            
            for trade in trades:
                asyncio.run(kafka_callback.write(trade))
            
            end_time = time.time()
            
            total_time = end_time - start_time
            throughput = batch_size / total_time
            
            performance_results[batch_size] = {
                'time': total_time,
                'throughput': throughput
            }
            
            # Verify all trades were processed
            self.assertEqual(len(self.mock_kafka_data), batch_size)
        
        # Performance analysis
        print(f"\nPerformance results for different batch sizes:")
        for batch_size, results in performance_results.items():
            print(f"Batch size {batch_size}: {results['throughput']:.2f} trades/sec")

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_latency_distribution(self, mock_producer_class):
        """Test latency distribution for trade processing"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        num_trades = 100
        latencies = []
        
        for i in range(num_trades):
            trade_data = {
                'symbol': 'BTC-USD',
                'exchange': 'COINBASE',
                'id': f'latency_test_{i}',
                'timestamp': time.time(),
                'amount': 0.1 + (i * 0.001),
                'price': 45000.0 + (i * 0.1),
                'side': 'buy' if i % 2 == 0 else 'sell',
                'type': 'trade',
                'receipt_timestamp': time.time() + 0.001
            }
            
            start_time = time.time()
            asyncio.run(kafka_callback.write(trade_data))
            end_time = time.time()
            
            latency = (end_time - start_time) * 1000  # Convert to milliseconds
            latencies.append(latency)
        
        # Calculate latency statistics
        avg_latency = statistics.mean(latencies)
        median_latency = statistics.median(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        p95_latency = statistics.quantiles(latencies, n=20)[18]  # 95th percentile
        
        # Verify all trades were processed
        self.assertEqual(len(self.mock_kafka_data), num_trades)
        
        # Performance assertions
        self.assertLess(avg_latency, 50)  # Average latency should be less than 50ms
        self.assertLess(p95_latency, 100)  # 95th percentile should be less than 100ms
        
        print(f"\nLatency statistics:")
        print(f"  Average: {avg_latency:.2f}ms")
        print(f"  Median: {median_latency:.2f}ms")
        print(f"  Min: {min_latency:.2f}ms")
        print(f"  Max: {max_latency:.2f}ms")
        print(f"  95th percentile: {p95_latency:.2f}ms")

    @patch('cryptofeed_tools.AIOKafkaProducer')
    def test_error_handling_performance(self, mock_producer_class):
        """Test performance when handling errors"""
        mock_producer = AsyncMock()
        mock_producer.send_and_wait.side_effect = self.mock_kafka_send
        mock_producer_class.return_value = mock_producer
        
        kafka_callback = cft.ClickHouseTradeKafka()
        kafka_callback.producer = mock_producer
        
        # Mix of valid and invalid trades
        trades = []
        for i in range(50):
            if i % 10 == 0:  # Every 10th trade is invalid
                # Invalid trade (missing required fields)
                trade = {
                    'symbol': 'BTC-USD',
                    'exchange': 'COINBASE',
                    'id': f'error_test_{i}',
                    # Missing timestamp, amount, price
                    'side': 'buy',
                    'type': 'trade',
                    'receipt_timestamp': time.time() + 0.001
                }
            else:
                # Valid trade
                trade = {
                    'symbol': 'BTC-USD',
                    'exchange': 'COINBASE',
                    'id': f'valid_test_{i}',
                    'timestamp': time.time(),
                    'amount': 0.1,
                    'price': 45000.0,
                    'side': 'buy',
                    'type': 'trade',
                    'receipt_timestamp': time.time() + 0.001
                }
            trades.append(trade)
        
        start_time = time.time()
        
        for trade in trades:
            asyncio.run(kafka_callback.write(trade))
        
        end_time = time.time()
        
        total_time = end_time - start_time
        throughput = len(trades) / total_time
        
        # Should still process valid trades despite errors
        self.assertGreater(len(self.mock_kafka_data), 0)
        self.assertLess(total_time, 10)  # Should complete in reasonable time
        self.assertGreater(throughput, 5)  # Should maintain reasonable throughput

    def test_symbol_processing_performance(self):
        """Test performance of processing different symbols"""
        # This test measures the performance impact of different symbol types
        symbols = cft.SYMBOLS + cft.SYMBOLS_HYPERLIQUID
        
        # Test symbol validation performance
        start_time = time.time()
        
        for symbol in symbols:
            # Simulate symbol validation
            if '-' in symbol:
                # Standard exchange symbol
                pass
            else:
                # HyperLiquid symbol
                pass
        
        end_time = time.time()
        
        validation_time = (end_time - start_time) * 1000  # Convert to milliseconds
        
        # Symbol validation should be very fast
        self.assertLess(validation_time, 10)  # Less than 10ms for all symbols


if __name__ == '__main__':
    unittest.main()
