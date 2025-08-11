#!/usr/bin/env python3
"""
Test different possible OrderBook data structures
"""

from collections import OrderedDict
from decimal import Decimal

def test_different_structures():
    """Test various possible data structures"""
    
    print("Testing different possible OrderBook data structures...")
    
    # Structure 1: What we expect (with 'book' field)
    structure_1 = {
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
    
    # Structure 2: Direct bid/ask (no 'book' wrapper)
    structure_2 = {
        'timestamp': 1754870444.0,
        'receipt_timestamp': 1754870444.1,
        'exchange': 'HYPERLIQUID',
        'symbol': 'BTC',
        'bid': {
            Decimal('119292.0'): Decimal('7.83756'),
            Decimal('119291.0'): Decimal('0.0009'),
        },
        'ask': {
            Decimal('119293.0'): Decimal('8.82288'),
            Decimal('119294.0'): Decimal('0.04405'),
        },
        'delta': None
    }
    
    # Structure 3: Nested with different keys
    structure_3 = {
        'timestamp': 1754870444.0,
        'receipt_timestamp': 1754870444.1,
        'exchange': 'HYPERLIQUID',
        'symbol': 'BTC',
        'bids': {
            Decimal('119292.0'): Decimal('7.83756'),
            Decimal('119291.0'): Decimal('0.0009'),
        },
        'asks': {
            Decimal('119293.0'): Decimal('8.82288'),
            Decimal('119294.0'): Decimal('0.04405'),
        },
        'delta': None
    }
    
    structures = [
        ("Structure 1 (with 'book')", structure_1),
        ("Structure 2 (direct bid/ask)", structure_2),
        ("Structure 3 (bids/asks)", structure_3)
    ]
    
    for name, data in structures:
        print(f"\n{'='*60}")
        print(f"Testing {name}")
        print(f"{'='*60}")
        
        try:
            # Test the processing logic
            result = process_orderbook_data(data)
            if result:
                print(f"✅ {name} processed successfully!")
                print(f"Final keys: {list(result.keys())}")
            else:
                print(f"❌ {name} failed processing")
                
        except Exception as e:
            print(f"❌ {name} failed with error: {e}")

def process_orderbook_data(data):
    """Process orderbook data like ClickHouseBookKafka would"""
    
    try:
        # Convert timestamps
        processed = data.copy()
        processed['ts'] = int(processed.pop('timestamp') * 1_000_000_000)
        processed['receipt_ts'] = int(processed.pop('receipt_timestamp') * 1_000_000_000)
        
        # Handle different possible book structures
        if 'book' in processed and 'bid' in processed['book'] and 'ask' in processed['book']:
            # Structure 1: with 'book' wrapper
            processed['bid'] = OrderedDict(sorted(processed['book'].pop('bid').items(), reverse=True))
            processed['ask'] = OrderedDict(sorted(processed['book'].pop('ask').items()))
            del processed['book']
            
        elif 'bid' in processed and 'ask' in processed:
            # Structure 2: direct bid/ask
            processed['bid'] = OrderedDict(sorted(processed['bid'].items(), reverse=True))
            processed['ask'] = OrderedDict(sorted(processed['ask'].items()))
            
        elif 'bids' in processed and 'asks' in processed:
            # Structure 3: bids/asks
            processed['bid'] = OrderedDict(sorted(processed.pop('bids').items(), reverse=True))
            processed['ask'] = OrderedDict(sorted(processed.pop('asks').items()))
            
        else:
            print(f"❌ Unknown book structure. Available fields: {list(processed.keys())}")
            return None
        
        # Clean up
        if 'delta' in processed:
            del processed['delta']
            
        return processed
        
    except Exception as e:
        print(f"❌ Processing failed: {e}")
        return None

if __name__ == "__main__":
    test_different_structures()
