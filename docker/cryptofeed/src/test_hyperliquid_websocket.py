#!/usr/bin/env python3
"""
Minimal test script to connect to HyperLiquid websocket once and log the JSON structure.
This avoids modifying the existing hyperliquid.py file.
"""

import asyncio
import json
from decimal import Decimal
import logging
from websockets import connect

# Configure logging to see everything
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
LOG = logging.getLogger(__name__)

def verify_l2book_transform(message_json: dict) -> dict:
    """Mirror hyperliquid.py's l2Book processing for a quick verification."""
    if message_json.get("channel") != "l2Book":
        return {"ok": False, "reason": "not l2Book"}

    data = message_json.get("data", {})
    coin = data.get("coin")
    levels = data.get("levels", [[], []])
    if not isinstance(levels, list) or len(levels) < 2:
        return {"ok": False, "reason": "levels missing/invalid"}

    bids_raw = levels[0]
    asks_raw = levels[1]

    try:
        bids = {Decimal(l["px"]) if isinstance(l.get("px"), str) else Decimal(str(l.get("px"))):
                Decimal(l["sz"]) if isinstance(l.get("sz"), str) else Decimal(str(l.get("sz")))
                for l in bids_raw}
        asks = {Decimal(l["px"]) if isinstance(l.get("px"), str) else Decimal(str(l.get("px"))):
                Decimal(l["sz"]) if isinstance(l.get("sz"), str) else Decimal(str(l.get("sz")))
                for l in asks_raw}
    except Exception as e:
        return {"ok": False, "reason": f"decimal parse error: {e}"}

    best_bid = max(bids) if bids else None
    best_ask = min(asks) if asks else None

    summary = {
        "ok": True,
        "pair": coin,
        "num_bids": len(bids),
        "num_asks": len(asks),
        "best_bid": str(best_bid) if best_bid is not None else None,
        "best_ask": str(best_ask) if best_ask is not None else None,
        "ts_ms": data.get("time"),
    }

    # Soft sanity check: best bid should be less than best ask when both exist
    if best_bid is not None and best_ask is not None and not (best_bid < best_ask):
        summary["ok"] = False
        summary["reason"] = "best_bid >= best_ask"

    return summary

async def test_hyperliquid_websocket():
    """Connect to HyperLiquid websocket once and log all messages"""
    
    # Test symbols from your existing code
    test_symbols = ["BTC", "ETH", "SOL"]
    
    uri = "wss://api.hyperliquid.xyz/ws"
    
    LOG.info(f"Connecting to {uri}")
    
    try:
        async with connect(uri) as websocket:
            LOG.info("Connected to HyperLiquid websocket")
            
            # Subscribe to L2 book for test symbols
            for symbol in test_symbols:
                subscribe_msg = {
                    "method": "subscribe",
                    "subscription": {
                        "type": "l2Book",
                        "coin": symbol
                    }
                }
                
                LOG.info(f"Sending subscribe message: {json.dumps(subscribe_msg, indent=2)}")
                await websocket.send(json.dumps(subscribe_msg))
                
                # Wait a bit for subscription confirmation
                await asyncio.sleep(1)
            
            # Listen for messages for a limited time
            LOG.info("Listening for messages (will timeout after 30 seconds)...")
            
            timeout = 30  # seconds
            start_time = asyncio.get_event_loop().time()
            
            while True:
                try:
                    # Set a timeout for receiving messages
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    
                    LOG.info("=" * 80)
                    LOG.info("RAW MESSAGE RECEIVED:")
                    LOG.info(message)
                    LOG.info("=" * 80)
                    
                    # Try to parse as JSON and pretty print
                    try:
                        parsed = json.loads(message)
                        LOG.info("PARSED JSON:")
                        LOG.info(json.dumps(parsed, indent=2))

                        # If this is an l2Book message, verify the transform
                        if parsed.get("channel") == "l2Book":
                            summary = verify_l2book_transform(parsed)
                            LOG.info("L2BOOK TRANSFORM SUMMARY: %s", json.dumps(summary, indent=2))
                            # Exit early after a successful verification to keep the test snappy
                            if summary.get("ok"):
                                LOG.info("L2Book transform verified; stopping test early.")
                                break
                    except json.JSONDecodeError as e:
                        LOG.warning(f"Failed to parse as JSON: {e}")
                    
                    LOG.info("=" * 80)
                    
                    # Check if we've been running too long
                    if asyncio.get_event_loop().time() - start_time > timeout:
                        LOG.info("Timeout reached, stopping...")
                        break
                        
                except asyncio.TimeoutError:
                    LOG.info("No message received in 5 seconds, continuing to listen...")
                    continue
                except Exception as e:
                    LOG.error(f"Error receiving message: {e}")
                    break
                    
    except Exception as e:
        LOG.error(f"Connection error: {e}")
    
    LOG.info("Test completed")

if __name__ == "__main__":
    asyncio.run(test_hyperliquid_websocket())

"""
2025-08-10 17:00:44,326 - INFO - ================================================================================
2025-08-10 17:00:44,326 - INFO - RAW MESSAGE RECEIVED:
2025-08-10 17:00:44,326 - INFO - {"channel":"l2Book","data":{"coin":"BTC","time":1754870444265,"levels":[[{"px":"119292.0","sz":"7.83756","n":24},{"px":"119291.0","sz":"0.0009","n":1},{"px":"119290.0","sz":"1.64482","n":3},{"px":"119288.0","sz":"0.05029","n":1},{"px":"119286.0","sz":"1.31","n":4},{"px":"119283.0","sz":"0.43488","n":1},{"px":"119282.0","sz":"0.02235","n":1},{"px":"119281.0","sz":"0.13355","n":1},{"px":"119280.0","sz":"0.5383","n":5},{"px":"119279.0","sz":"1.76021","n":3},{"px":"119278.0","sz":"4.71096","n":3},{"px":"119277.0","sz":"0.00011","n":1},{"px":"119276.0","sz":"4.19094","n":3},{"px":"119275.0","sz":"1.32376","n":4},{"px":"119274.0","sz":"0.91875","n":4},{"px":"119273.0","sz":"4.27608","n":2},{"px":"119272.0","sz":"0.441","n":3},{"px":"119271.0","sz":"1.08604","n":4},{"px":"119270.0","sz":"4.64041","n":13},{"px":"119269.0","sz":"0.00011","n":1}],[{"px":"119293.0","sz":"8.82288","n":9},{"px":"119294.0","sz":"0.04405","n":2},{"px":"119296.0","sz":"0.00011","n":1},{"px":"119297.0","sz":"0.00052","n":2},{"px":"119298.0","sz":"0.02846","n":2},{"px":"119299.0","sz":"0.00011","n":1},{"px":"119300.0","sz":"0.61147","n":4},{"px":"119301.0","sz":"0.61159","n":3},{"px":"119302.0","sz":"0.06438","n":5},{"px":"119303.0","sz":"0.00052","n":2},{"px":"119304.0","sz":"0.49558","n":2},{"px":"119305.0","sz":"0.08392","n":2},{"px":"119306.0","sz":"0.43499","n":2},{"px":"119307.0","sz":"0.11933","n":4},{"px":"119308.0","sz":"0.12111","n":2},{"px":"119309.0","sz":"0.51916","n":4},{"px":"119310.0","sz":"1.03221","n":4},{"px":"119311.0","sz":"0.44011","n":2},{"px":"119312.0","sz":"0.84693","n":3},{"px":"119313.0","sz":"14.49836","n":9}]]}}
2025-08-10 17:00:44,326 - INFO - ================================================================================
"""