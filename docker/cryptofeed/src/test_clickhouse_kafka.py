#!/usr/bin/env python3
"""
Test ClickHouseBookKafka processing logic directly
"""

import asyncio
from collections import OrderedDict
import orjson

# Import your local classes
from cryptofeed_tools import ClickHouseBookKafka

async def test_clickhouse_kafka():
    """Test the ClickHouseBookKafka data processing"""
    
    # Create instance (we won't actually connect to Kafka)
    ch_kafka = ClickHouseBookKafka(bootstrap='localhost', port=9092)
    
    # Mock data that might come from cryptofeed OrderBook.to_dict()
    mock_data_1 = {
        'timestamp': 1754870444.0,
        'receipt_timestamp': 1754870444.1,
        'exchange': 'HYPERLIQUID',
        'symbol': 'BTC',
        'book': {
            'bid': {
                Decimal('119292.0'): Decimal('7.83756'),
                Decimal('119291.0'): Decimal('0.0009'),
            },
            'ask': {
                Decimal('119293.0'): Decimal('8.82288'),
                Decimal('119294.0'): Decimal('0.04405'),
            }
        },
        'delta': None
    }
    
    mock_data_2 = {
        'timestamp': 1754870444.0,
        'receipt_timestamp': 1754870444.1,
        'exchange': 'HYPERLIQUID',
        'symbol': 'BTC',
        'book': {
            'bid': OrderedDict([
                (Decimal('119292.0'), Decimal('7.83756')),
                (Decimal('119291.0'), Decimal('0.0009')),
            ]),
            'ask': OrderedDict([
                (Decimal('119293.0'), Decimal('8.82288')),
                (Decimal('119294.0'), Decimal('0.04405')),
            ])
        },
        'delta': None
    }
    
    print("Testing ClickHouseBookKafka.write() with different data structures...")
    
    # Test first mock data
    print("\n" + "="*50)
    print("TESTING MOCK DATA 1:")
    print(f"Data structure: {mock_data_1}")
    
    try:
        # Mock the producer to avoid actual Kafka connection
        ch_kafka.producer = type('MockProducer', (), {
            'send_and_wait': lambda topic, data: print(f"Would send to {topic}: {data[:100]}...")
        })()
        
        await ch_kafka.write(mock_data_1)
        print("✅ Mock data 1 processed successfully")
        
    except Exception as e:
        print(f"❌ Mock data 1 failed: {e}")
    
    # Test second mock data
    print("\n" + "="*50)
    print("TESTING MOCK DATA 2:")
    print(f"Data structure: {mock_data_2}")
    
    try:
        await ch_kafka.write(mock_data_2)
        print("✅ Mock data 2 processed successfully")
        
    except Exception as e:
        print(f"❌ Mock data 2 failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_clickhouse_kafka())
