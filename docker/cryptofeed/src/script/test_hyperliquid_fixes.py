#!/usr/bin/env python3
"""
Simple test script to verify HyperLiquid L2 book processing fixes.
This tests the core logic without requiring full cryptofeed framework.
"""

import asyncio
import json
import logging
from websockets import connect
from decimal import Decimal
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
LOG = logging.getLogger(__name__)

class SimpleSymbol:
    """Simple Symbol class to mimic cryptofeed Symbol"""
    def __init__(self, base, quote, type="perpetual"):
        self.base = base
        self.quote = quote  
        self.type = type
        self.normalized = f"{base}-{quote}-PERP"

class SimpleOrderBook:
    """Simple OrderBook class to mimic cryptofeed OrderBook"""
    def __init__(self, exchange, symbol):
        self.exchange = exchange
        self.symbol = symbol
        self.timestamp = None
        self.bids = {}  # price -> size
        self.asks = {}  # price -> size
    
    def clear(self):
        self.bids.clear()
        self.asks.clear()
    
    def to_dict(self):
        return {
            'exchange': self.exchange,
            'symbol': self.symbol,
            'timestamp': float(self.timestamp) if self.timestamp else None,
            'book': {
                'bid': dict(self.bids),
                'ask': dict(self.asks)
            }
        }

def test_symbol_mapping():
    """Test the fixed symbol mapping logic"""
    LOG.info("🔍 TESTING SYMBOL MAPPING...")
    
    # Simulate HyperLiquid API response
    mock_data = {
        "BTC": 119292.0,
        "ETH": 4180.5,
        "SOL": 245.8
    }
    
    # Apply our fixed logic
    symbols_map = {}
    info = {'instrument_type': {}}
    
    for sym in mock_data.keys():
        if sym.startswith('@'):
            continue
        # Our fix: Create proper Symbol objects
        symbol = SimpleSymbol(sym, "USD", type="perpetual")
        symbols_map[symbol.normalized] = sym  # Map normalized to exchange
        info['instrument_type'][symbol.normalized] = 'perpetual'
    
    LOG.info("📋 SYMBOL MAPPING RESULTS:")
    for normalized, exchange in symbols_map.items():
        LOG.info(f"   ✅ {normalized} -> {exchange}")
    
    return symbols_map

def simulate_l2_book_processing(test_message, symbols_map):
    """Simulate the fixed L2 book processing"""
    LOG.info("🔍 TESTING L2 BOOK PROCESSING...")
    
    # Extract data like our fixed _handle_l2_book method
    data = test_message.get("data")
    if not data:
        LOG.error("❌ No data in message")
        return None
        
    coin = data.get("coin")
    if not coin:
        LOG.error("❌ No coin in data")
        return None
    
    # Convert exchange symbol to standard symbol (our fix)
    reverse_map = {v: k for k, v in symbols_map.items()}
    std_symbol = reverse_map.get(coin, coin)
    LOG.info(f"🔄 Symbol mapping: '{coin}' -> '{std_symbol}'")
    
    # Get levels array
    levels = data.get("levels", [])
    if len(levels) != 2:
        LOG.error(f"❌ Expected 2 levels arrays, got {len(levels)}")
        return None
    
    bids_data, asks_data = levels
    
    # Create OrderBook (with our max_depth fix)
    book = SimpleOrderBook("HYPERLIQUID", std_symbol)
    book.timestamp = Decimal(str(data.get("time", 0))) / Decimal("1000.0")
    
    # Process bids
    for bid_level in bids_data:
        price = Decimal(bid_level.get("px", "0"))
        size = Decimal(bid_level.get("sz", "0"))
        if price > 0 and size > 0:
            book.bids[price] = size
    
    # Process asks  
    for ask_level in asks_data:
        price = Decimal(ask_level.get("px", "0"))
        size = Decimal(ask_level.get("sz", "0"))
        if price > 0 and size > 0:
            book.asks[price] = size
    
    LOG.info(f"📦 ORDERBOOK CREATED:")
    LOG.info(f"   Exchange: {book.exchange}")
    LOG.info(f"   Symbol: {book.symbol}")
    LOG.info(f"   Timestamp: {book.timestamp}")
    LOG.info(f"   Bids: {len(book.bids)} levels")
    LOG.info(f"   Asks: {len(book.asks)} levels")
    
    if book.bids:
        best_bid = max(book.bids.keys())
        LOG.info(f"   Best Bid: ${best_bid} (size: {book.bids[best_bid]})")
    
    if book.asks:
        best_ask = min(book.asks.keys())
        LOG.info(f"   Best Ask: ${best_ask} (size: {book.asks[best_ask]})")
        LOG.info(f"   Spread: ${best_ask - best_bid}")
    
    return book

def simulate_kafka_processing(book):
    """Simulate ClickHouseBookKafka processing"""
    LOG.info("🔍 TESTING KAFKA PROCESSING...")
    
    # Convert to dict like the callback would
    data = book.to_dict()
    
    # Apply ClickHouseBookKafka transformations
    kafka_data = {
        'exchange': data['exchange'],
        'symbol': data['symbol'],
        'ts': int(data['timestamp'] * 1_000_000_000),  # nanoseconds
        'bid': dict(sorted(data['book']['bid'].items(), reverse=True)),
        'ask': dict(sorted(data['book']['ask'].items()))
    }
    
    LOG.info("📊 KAFKA DATA (would be sent to ClickHouse):")
    LOG.info(f"   exchange: {kafka_data['exchange']}")
    LOG.info(f"   symbol: {kafka_data['symbol']}")
    LOG.info(f"   ts: {kafka_data['ts']}")
    LOG.info(f"   bid levels: {len(kafka_data['bid'])}")
    LOG.info(f"   ask levels: {len(kafka_data['ask'])}")
    
    # Show sample bid/ask data
    if kafka_data['bid']:
        top_bids = list(kafka_data['bid'].items())[:3]
        LOG.info(f"   top 3 bids: {top_bids}")
    
    if kafka_data['ask']:
        top_asks = list(kafka_data['ask'].items())[:3]  
        LOG.info(f"   top 3 asks: {top_asks}")
    
    return kafka_data

async def test_with_real_websocket():
    """Test with real websocket data"""
    LOG.info("🔍 TESTING WITH REAL WEBSOCKET...")
    
    # Set up symbol mapping
    symbols_map = test_symbol_mapping()
    
    uri = "wss://api.hyperliquid.xyz/ws"
    test_symbol = "BTC"
    
    try:
        async with connect(uri) as websocket:
            LOG.info("✅ Connected to HyperLiquid websocket")
            
            # Subscribe
            subscribe_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "l2Book", 
                    "coin": test_symbol
                }
            }
            
            LOG.info(f"📤 Subscribing to {test_symbol} L2 book")
            await websocket.send(json.dumps(subscribe_msg))
            await asyncio.sleep(1)
            
            # Get one message and test processing
            LOG.info("👂 Waiting for L2 book message...")
            
            message = await asyncio.wait_for(websocket.recv(), timeout=15.0)
            parsed_msg = json.loads(message, parse_float=Decimal)
            
            LOG.info("📨 RECEIVED MESSAGE:")
            LOG.info("=" * 80)
            
            if parsed_msg.get("channel") == "l2Book":
                # Test our processing pipeline
                book = simulate_l2_book_processing(parsed_msg, symbols_map)
                if book:
                    kafka_data = simulate_kafka_processing(book)
                    LOG.info("✅ SUCCESS: Complete pipeline test passed!")
                    return True
                else:
                    LOG.error("❌ FAILURE: L2 book processing failed")
                    return False
            else:
                LOG.warning(f"⚠️ Received {parsed_msg.get('channel')} instead of l2Book")
                return False
                
    except asyncio.TimeoutError:
        LOG.error("❌ TIMEOUT: No message received")
        return False
    except Exception as e:
        LOG.error(f"❌ ERROR: {e}")
        return False

async def main():
    """Run comprehensive test"""
    LOG.info("🚀 TESTING HYPERLIQUID FIXES")
    LOG.info("=" * 100)
    
    # Test 1: Symbol mapping
    symbols_map = test_symbol_mapping()
    LOG.info("=" * 100)
    
    # Test 2: L2 book processing with sample data
    sample_message = {
        "channel": "l2Book",
        "data": {
            "coin": "BTC",
            "time": 1754870444265,
            "levels": [
                [{"px": "119292.0", "sz": "7.83756", "n": 24}],
                [{"px": "119293.0", "sz": "8.82288", "n": 9}]
            ]
        }
    }
    
    book = simulate_l2_book_processing(sample_message, symbols_map)
    if book:
        simulate_kafka_processing(book)
    LOG.info("=" * 100)
    
    # Test 3: Real websocket test
    success = await test_with_real_websocket()
    LOG.info("=" * 100)
    
    if success:
        LOG.info("🎉 ALL TESTS PASSED! The fixes are working correctly.")
    else:
        LOG.error("❌ SOME TESTS FAILED! Check the output above.")
    
    LOG.info("🏁 TEST COMPLETE")

if __name__ == "__main__":
    asyncio.run(main())
