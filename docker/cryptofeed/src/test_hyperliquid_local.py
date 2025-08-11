#!/usr/bin/env python3
"""
Test just the local HyperLiquid class to see the data structure
"""

import asyncio
import json
from decimal import Decimal

# Import your local hyperliquid class
from hyperliquid import HyperLiquid

async def test_local_hyperliquid():
    """Test the local HyperLiquid class data structure"""
    
    # Create instance
    hl = HyperLiquid()
    
    # Mock the data structure from your websocket test
    mock_msg = {
        "channel": "l2Book",
        "data": {
            "coin": "BTC",
            "time": 1754870444265,
            "levels": [
                [{"px": "119292.0", "sz": "7.83756", "n": 24}],  # bids
                [{"px": "119293.0", "sz": "8.82288", "n": 9}]   # asks
            ]
        }
    }
    
    print("Testing HyperLiquid._handle_l2_book with mock data...")
    print(f"Mock message: {json.dumps(mock_msg, indent=2)}")
    
    # Test the _handle_l2_book method
    timestamp = 1754870444.0
    
    # Mock callback to capture what would be sent
    captured_data = []
    
    async def mock_callback(channel, data, ts):
        print(f"\nMOCK CALLBACK CALLED:")
        print(f"Channel: {channel}")
        print(f"Data type: {type(data)}")
        print(f"Data attributes: {dir(data)}")
        
        # Try to convert to dict to see structure
        try:
            data_dict = data.to_dict()
            print(f"Data as dict keys: {list(data_dict.keys())}")
            if 'book' in data_dict:
                print(f"Book structure: {data_dict['book']}")
            captured_data.append(data_dict)
        except Exception as e:
            print(f"Error converting to dict: {e}")
    
    # Replace the callback temporarily
    original_callback = hl.book_callback
    hl.book_callback = mock_callback
    
    try:
        # Call the method
        await hl._handle_l2_book(mock_msg, timestamp)
        
        print(f"\nCaptured data: {captured_data}")
        
    finally:
        # Restore original callback
        hl.book_callback = original_callback

if __name__ == "__main__":
    asyncio.run(test_local_hyperliquid())
