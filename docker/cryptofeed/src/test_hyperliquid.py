#!/usr/bin/env python3
import asyncio
import json
import time
import websockets
from pathlib import Path
from collections import OrderedDict
from typing import Dict, List, Any, Optional

class HyperLiquidTester:
    def __init__(self, output_dir: str = None):
        """Initialize the HyperLiquid tester with output directory"""
        if output_dir is None:
            self.output_dir = Path("/home/alex/prod_trading/deephaven-clickhouse-streaming/docker/clickhouse/example_data")
        else:
            self.output_dir = Path(output_dir)
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.uri = "wss://api.hyperliquid.xyz/ws"
        self.timestamp = int(time.time())
    
    async def test_orderbook(self, symbol: str = "BTC") -> Dict[str, Any]:
        """Test L2 orderbook subscription and data processing"""
        print(f"\n=== TESTING ORDERBOOK FOR {symbol} ===")
        
        try:
            async with websockets.connect(self.uri) as websocket:
                # Subscribe to L2 orderbook
                subscribe_msg = {
                    "method": "subscribe",
                    "subscription": {
                        "type": "l2Book",
                        "coin": symbol
                    }
                }
                
                await websocket.send(json.dumps(subscribe_msg))
                print(f"Subscribed to {symbol} L2 orderbook")
                
                # Wait for response
                response = await websocket.recv()
                print(f"Received: {response}")
                
                # Wait for actual L2 data
                l2_data = await websocket.recv()
                print(f"Received L2 data: {l2_data}")
                
                # Parse and save data
                raw_data = json.loads(l2_data)
                formatted_data = self._format_orderbook_data(raw_data, symbol)
                
                # Save files
                raw_file = self.output_dir / f"l2_raw_{self.timestamp}.json"
                output_file = self.output_dir / f"l2_output_{self.timestamp}.json"
                
                with open(raw_file, 'w') as f:
                    json.dump(raw_data, f, indent=2)
                
                with open(output_file, 'w') as f:
                    json.dump(formatted_data, f, indent=2)
                
                print(f"Saved L2 data to {raw_file} and {output_file}")
                
                # Verify data processing
                self.verify_orderbook_data(formatted_data)
                
                return {
                    "raw_data": raw_data,
                    "formatted_data": formatted_data,
                    "raw_file": str(raw_file),
                    "output_file": str(output_file)
                }
                
        except Exception as e:
            print(f"❌ Orderbook test failed: {e}")
            return {"error": str(e)}
    
    # ... existing code ...
    
    async def test_trades(self, symbol: str = "BTC") -> Dict[str, Any]:
        """Test trades subscription and data processing"""
        print(f"\n=== TESTING TRADES FOR {symbol} ===")
        
        try:
            async with websockets.connect(self.uri) as websocket:
                # Subscribe to trades
                subscribe_msg = {
                    "method": "subscribe",
                    "subscription": {
                        "type": "trades",
                        "coin": symbol
                    }
                }
                
                await websocket.send(json.dumps(subscribe_msg))
                print(f"Subscribed to {symbol} trades")
                
                # Wait for trades response
                response = await websocket.recv()
                print(f"Received: {response}")
                
                # Wait for actual trades data
                trades_data = await websocket.recv()
                print(f"Received trades data: {trades_data}")
                
                # Parse and save data
                raw_data = json.loads(trades_data)
                formatted_data = self._format_trades_data(raw_data, symbol)
                
                # Save files
                raw_file = self.output_dir / f"trades_raw_{self.timestamp}.json"
                output_file = self.output_dir / f"trades_output_{self.timestamp}.json"
                
                with open(raw_file, 'w') as f:
                    json.dump(raw_data, f, indent=2)
                
                with open(output_file, 'w') as f:
                    json.dump(formatted_data, f, indent=2)
                
                print(f"Saved trades data to {raw_file} and {output_file}")
                
                # Verify data processing
                self.verify_trades_data(formatted_data)
                
                return {
                    "raw_data": raw_data,
                    "formatted_data": formatted_data,
                    "raw_file": str(raw_file),
                    "output_file": str(output_file)
                }
                
        except Exception as e:
            print(f"❌ Trades test failed: {e}")
            return {"error": str(e)}
    
    async def test_both(self, symbol: str = "BTC") -> Dict[str, Any]:
        """Test both orderbook and trades in sequence"""
        print(f"\n=== TESTING BOTH ORDERBOOK AND TRADES FOR {symbol} ===")
        
        results = {}
        
        # Test orderbook first
        orderbook_result = await self.test_orderbook(symbol)
        results["orderbook"] = orderbook_result
        
        # Test trades second
        trades_result = await self.test_trades(symbol)
        results["trades"] = trades_result
        
        return results
    
    def _format_orderbook_data(self, raw_data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        """Format raw orderbook data into standardized structure"""
        if "data" in raw_data and "levels" in raw_data["data"]:
            levels = raw_data["data"]["levels"]
            exchange_timestamp = raw_data["data"].get("time")
            
            return {
                "symbol": symbol,
                "timestamp": exchange_timestamp,
                "bids": {level["px"]: level["sz"] for level in levels[0] if len(levels) > 0},
                "asks": {level["px"]: level["sz"] for level in levels[1] if len(levels) > 1}
            }
        else:
            return {"error": "Unexpected data format", "raw": raw_data}
    
    def _format_trades_data(self, raw_data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        """Format raw trades data into standardized structure"""
        if "data" in raw_data and isinstance(raw_data["data"], list):
            trades = raw_data["data"]
            formatted_trades = []
            
            for trade in trades:
                formatted_trade = {
                    "symbol": symbol,
                    "timestamp": trade.get("time"),
                    "side": trade.get("side"),
                    "price": trade.get("px"),
                    "size": trade.get("sz"),
                    "trade_id": trade.get("tid"),
                    "hash": trade.get("hash"),
                    "buyer": trade.get("users", [None, None])[0] if trade.get("users") else None,
                    "seller": trade.get("users", [None, None])[1] if trade.get("users") else None
                }
                formatted_trades.append(formatted_trade)
            
            return {
                "trades": formatted_trades,
                "count": len(formatted_trades)
            }
        else:
            return {"error": "Unexpected trades format", "raw": raw_data}
    
    def verify_orderbook_data(self, formatted_data: Dict[str, Any]) -> None:
        """Verify that the formatted orderbook data can be processed using ClickHouseBookKafka logic"""
        print("\n=== VERIFYING ORDERBOOK DATA PROCESSING ===")
        
        try:
            if "error" in formatted_data:
                print(f"❌ Orderbook data has error: {formatted_data['error']}")
                return
            
            # Create a copy to avoid modifying the original
            data = formatted_data.copy()
            
            # Add required fields that ClickHouseBookKafka expects
            data['receipt_timestamp'] = time.time()
            data['book'] = {
                'bid': data.pop('bids'),
                'ask': data.pop('asks')
            }
            
            print(f"Original data structure: {json.dumps(data, indent=2)}")
            
            # Apply ClickHouseBookKafka processing logic
            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            data['bid'] = OrderedDict(sorted(data['book'].pop('bid').items(), reverse=True))
            data['ask'] = OrderedDict(sorted(data['book'].pop('ask').items()))
            del data['book']
            
            print(f"\nProcessed data structure: {json.dumps(data, indent=2)}")
            print("\n✅ Orderbook data processing successful!")
            
            # Verify the processed data has the expected structure
            expected_keys = {'symbol', 'ts', 'receipt_ts', 'bid', 'ask'}
            actual_keys = set(data.keys())
            
            if expected_keys.issubset(actual_keys):
                print("✅ All expected keys present")
            else:
                missing = expected_keys - actual_keys
                extra = actual_keys - expected_keys
                if missing:
                    print(f"❌ Missing keys: {missing}")
                if extra:
                    print(f"⚠️  Extra keys: {extra}")
            
            # Verify bid/ask are OrderedDicts and sorted correctly
            if isinstance(data['bid'], OrderedDict) and isinstance(data['ask'], OrderedDict):
                print("✅ Bid/Ask are OrderedDicts")
                
                # Check bid sorting (should be descending)
                bid_prices = list(data['bid'].keys())
                if bid_prices == sorted(bid_prices, reverse=True):
                    print("✅ Bids are sorted in descending order (highest first)")
                else:
                    print("❌ Bids are not sorted correctly")
                
                # Check ask sorting (should be ascending)
                ask_prices = list(data['ask'].keys())
                if ask_prices == sorted(ask_prices):
                    print("✅ Asks are sorted in ascending order (lowest first)")
                else:
                    print("❌ Asks are not sorted correctly")
            else:
                print("❌ Bid/Ask are not OrderedDicts")
                
        except Exception as e:
            print(f"❌ Orderbook data processing failed: {e}")
            import traceback
            traceback.print_exc()
    
    def verify_trades_data(self, formatted_trades_data: Dict[str, Any]) -> None:
        """Verify that the trades data can be processed correctly"""
        print("\n=== VERIFYING TRADES DATA PROCESSING ===")
        
        try:
            if "error" in formatted_trades_data:
                print(f"❌ Trades data has error: {formatted_trades_data['error']}")
                return
            
            trades = formatted_trades_data.get("trades", [])
            print(f"Processing {len(trades)} trades")
            
            if not trades:
                print("⚠️  No trades to process")
                return
            
            # Process each trade to verify structure
            processed_trades = []
            
            for i, trade in enumerate(trades):
                print(f"\n--- Trade {i+1} ---")
                print(f"Original: {json.dumps(trade, indent=2)}")
                
                # Create a copy for processing
                processed_trade = trade.copy()
                
                # Verify required fields
                required_fields = ['symbol', 'timestamp', 'side', 'price', 'size', 'trade_id']
                missing_fields = [field for field in required_fields if not processed_trade.get(field)]
                
                if missing_fields:
                    print(f"❌ Missing required fields: {missing_fields}")
                    continue
                
                # Verify data types and values
                try:
                    # Convert timestamp to nanoseconds (similar to ClickHouse processing)
                    if processed_trade['timestamp']:
                        processed_trade['ts'] = int(processed_trade['timestamp'] * 1_000_000_000)
                        del processed_trade['timestamp']
                    
                    # Verify side is valid
                    if processed_trade['side'] not in ['B', 'A']:
                        print(f"⚠️  Unexpected side value: {processed_trade['side']}")
                    
                    # Verify price and size are numeric
                    price = float(processed_trade['price'])
                    size = float(processed_trade['size'])
                    
                    if price <= 0:
                        print(f"❌ Invalid price: {price}")
                    if size <= 0:
                        print(f"❌ Invalid size: {size}")
                    
                    # Verify trade_id is numeric
                    trade_id = int(processed_trade['trade_id'])
                    if trade_id < 0:
                        print(f"⚠️  Unexpected trade_id: {trade_id}")
                    
                    # Add receipt timestamp
                    processed_trade['receipt_ts'] = int(time.time() * 1_000_000_000)
                    
                    processed_trades.append(processed_trade)
                    print(f"✅ Trade {i+1} processed successfully")
                    print(f"Processed: {json.dumps(processed_trade, indent=2)}")
                    
                except (ValueError, TypeError) as e:
                    print(f"❌ Trade {i+1} processing failed: {e}")
                    continue
            
            # Summary
            print(f"\n=== TRADES PROCESSING SUMMARY ===")
            print(f"Total trades: {len(trades)}")
            print(f"Successfully processed: {len(processed_trades)}")
            print(f"Failed: {len(trades) - len(processed_trades)}")
            
            if processed_trades:
                # Verify final structure
                expected_keys = {'symbol', 'ts', 'receipt_ts', 'side', 'price', 'size', 'trade_id', 'hash', 'buyer', 'seller'}
                sample_trade = processed_trades[0]
                actual_keys = set(sample_trade.keys())
                
                if expected_keys.issubset(actual_keys):
                    print("✅ All expected keys present in processed trades")
                else:
                    missing = expected_keys - actual_keys
                    extra = actual_keys - expected_keys
                    if missing:
                        print(f"❌ Missing keys: {missing}")
                    if extra:
                        print(f"⚠️  Extra keys: {extra}")
            
        except Exception as e:
            print(f"❌ Trades verification failed: {e}")
            import traceback
            traceback.print_exc()

async def main():
    """Main function to run tests"""
    tester = HyperLiquidTester()
    
    # Test both orderbook and trades
    results = await tester.test_both("BTC")
    
    print("\n=== FINAL RESULTS ===")
    print(json.dumps(results, indent=2, default=str))

if __name__ == "__main__":
    asyncio.run(main())