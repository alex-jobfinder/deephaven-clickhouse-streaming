#!/usr/bin/env python3
"""
Super simple test - no external dependencies
"""

from collections import OrderedDict
from decimal import Decimal

def test_data_processing():
    """Test the data processing logic that would be in ClickHouseBookKafka.write()"""
    
    print("Testing data processing logic...")
    
    # Mock data that might come from cryptofeed OrderBook.to_dict()
    mock_data = {
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
    
    print(f"Original data keys: {list(mock_data.keys())}")
    print(f"Book structure: {mock_data['book']}")
    
    try:
        # Simulate the ClickHouseBookKafka.write() logic
        print("\nProcessing data...")
        
        # Convert timestamps
        data = mock_data.copy()
        data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
        data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
        
        print(f"After timestamp conversion: {list(data.keys())}")
        
        # Process book data
        if 'book' in data and 'bid' in data['book'] and 'ask' in data['book']:
            data['bid'] = OrderedDict(sorted(data['book'].pop('bid').items(), reverse=True))
            data['ask'] = OrderedDict(sorted(data['book'].pop('ask').items()))
            print(f"After book processing: {list(data.keys())}")
            print(f"Bid structure: {data['bid']}")
            print(f"Ask structure: {data['ask']}")
        else:
            print("❌ Missing 'book', 'bid', or 'ask' fields!")
            print(f"Available fields: {list(data.keys())}")
            if 'book' in data:
                print(f"Book field type: {type(data['book'])}")
                print(f"Book field content: {data['book']}")
        
        # Clean up
        del data['book']
        del data['delta']
        
        print(f"Final processed data keys: {list(data.keys())}")
        print("✅ Data processing successful!")
        
    except Exception as e:
        print(f"❌ Data processing failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_data_processing()
