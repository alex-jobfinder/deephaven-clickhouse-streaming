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


python /home/alex/prod_trading/deephaven-clickhouse-streaming/docker/cryptofeed/src/test_hyperliquid_websocket.py
2025-08-11 01:07:49,833 - DEBUG - Using selector: EpollSelector
2025-08-11 01:07:49,834 - INFO - Connecting to wss://api.hyperliquid.xyz/ws
2025-08-11 01:07:49,886 - DEBUG - = connection is CONNECTING
2025-08-11 01:07:49,911 - DEBUG - > GET /ws HTTP/1.1
2025-08-11 01:07:49,911 - DEBUG - > Host: api.hyperliquid.xyz
2025-08-11 01:07:49,911 - DEBUG - > Upgrade: websocket
2025-08-11 01:07:49,911 - DEBUG - > Connection: Upgrade
2025-08-11 01:07:49,912 - DEBUG - > Sec-WebSocket-Key: KtvKERGqMACVCeyB/4hWCA==
2025-08-11 01:07:49,912 - DEBUG - > Sec-WebSocket-Version: 13
2025-08-11 01:07:49,912 - DEBUG - > Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits
2025-08-11 01:07:49,912 - DEBUG - > User-Agent: Python/3.10 websockets/15.0.1
2025-08-11 01:07:50,130 - DEBUG - < HTTP/1.1 101 Switching Protocols
2025-08-11 01:07:50,130 - DEBUG - < Connection: upgrade
2025-08-11 01:07:50,130 - DEBUG - < Server: nginx/1.22.1
2025-08-11 01:07:50,130 - DEBUG - < Date: Mon, 11 Aug 2025 08:07:50 GMT
2025-08-11 01:07:50,131 - DEBUG - < upgrade: websocket
2025-08-11 01:07:50,131 - DEBUG - < sec-websocket-accept: KqV0xd3LtnSb0QGgX2SvGfvI9Z0=
2025-08-11 01:07:50,131 - DEBUG - < access-control-allow-origin: *
2025-08-11 01:07:50,131 - DEBUG - < vary: origin
2025-08-11 01:07:50,131 - DEBUG - < vary: access-control-request-method
2025-08-11 01:07:50,131 - DEBUG - < vary: access-control-request-headers
2025-08-11 01:07:50,131 - DEBUG - < access-control-expose-headers: *
2025-08-11 01:07:50,131 - DEBUG - < X-Cache: Miss from cloudfront
2025-08-11 01:07:50,131 - DEBUG - < Via: 1.1 9a2c15e5fae551ba78d35ae4b56282e8.cloudfront.net (CloudFront)
2025-08-11 01:07:50,131 - DEBUG - < X-Amz-Cf-Pop: LAX50-P5
2025-08-11 01:07:50,131 - DEBUG - < X-Amz-Cf-Id: jdpkgfix2h0DY288jAek5iJiaIG4HMbz2rtTBgVcHa99hDsjsRtm9w==
2025-08-11 01:07:50,131 - DEBUG - = connection is OPEN
2025-08-11 01:07:50,131 - INFO - Connected to HyperLiquid websocket
2025-08-11 01:07:50,131 - INFO - Sending subscribe message: {
  "method": "subscribe",
  "subscription": {
    "type": "l2Book",
    "coin": "BTC"
  }
}
2025-08-11 01:07:50,131 - DEBUG - > TEXT '{"method": "subscribe", "subscription": {"type"...2Book", "coin": "BTC"}}' [74 bytes]
2025-08-11 01:07:50,249 - DEBUG - < TEXT '{"channel":"subscriptionResponse","data":{"meth...null,"mantissa":null}}}' [142 bytes]
2025-08-11 01:07:50,249 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"BTC","time"...z":"3.84976","n":4}]]}}' [1635 bytes]
2025-08-11 01:07:50,675 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"BTC","time"...z":"1.96541","n":3}]]}}' [1633 bytes]
2025-08-11 01:07:51,132 - INFO - Sending subscribe message: {
  "method": "subscribe",
  "subscription": {
    "type": "l2Book",
    "coin": "ETH"
  }
}
2025-08-11 01:07:51,133 - DEBUG - > TEXT '{"method": "subscribe", "subscription": {"type"...2Book", "coin": "ETH"}}' [74 bytes]
2025-08-11 01:07:51,303 - DEBUG - < TEXT '{"channel":"subscriptionResponse","data":{"meth...null,"mantissa":null}}}' [142 bytes]
2025-08-11 01:07:51,303 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"ETH","time"...":"25.6727","n":15}]]}}' [1564 bytes]
2025-08-11 01:07:51,451 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"BTC","time"...z":"0.77364","n":3}]]}}' [1642 bytes]
2025-08-11 01:07:51,458 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"ETH","time"...":"114.1666","n":6}]]}}' [1551 bytes]
2025-08-11 01:07:51,810 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"BTC","time"...z":"1.50817","n":4}]]}}' [1644 bytes]
2025-08-11 01:07:51,831 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"ETH","time"...":"205.3122","n":8}]]}}' [1561 bytes]
2025-08-11 01:07:52,134 - INFO - Sending subscribe message: {
  "method": "subscribe",
  "subscription": {
    "type": "l2Book",
    "coin": "SOL"
  }
}
2025-08-11 01:07:52,134 - DEBUG - > TEXT '{"method": "subscribe", "subscription": {"type"...2Book", "coin": "SOL"}}' [74 bytes]
2025-08-11 01:07:52,395 - DEBUG - < TEXT '{"channel":"subscriptionResponse","data":{"meth...null,"mantissa":null}}}' [142 bytes]
2025-08-11 01:07:52,395 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"SOL","time"...sz":"566.99","n":7}]]}}' [1549 bytes]
2025-08-11 01:07:52,443 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"BTC","time"...z":"0.01379","n":1}]]}}' [1630 bytes]
2025-08-11 01:07:52,495 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"ETH","time"...z":"17.3593","n":6}]]}}' [1566 bytes]
2025-08-11 01:07:52,499 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"SOL","time"...sz":"566.99","n":7}]]}}' [1549 bytes]
2025-08-11 01:07:52,880 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"BTC","time"...z":"0.81267","n":2}]]}}' [1631 bytes]
2025-08-11 01:07:52,880 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"ETH","time"...z":"48.0962","n":7}]]}}' [1568 bytes]
2025-08-11 01:07:52,881 - DEBUG - < TEXT '{"channel":"l2Book","data":{"coin":"SOL","time"...sz":"583.45","n":9}]]}}' [1552 bytes]
2025-08-11 01:07:53,136 - INFO - Listening for messages (will timeout after 30 seconds)...
2025-08-11 01:07:53,137 - INFO - ================================================================================
2025-08-11 01:07:53,137 - INFO - RAW MESSAGE RECEIVED:
2025-08-11 01:07:53,137 - INFO - {"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":null,"mantissa":null}}}
2025-08-11 01:07:53,137 - INFO - ================================================================================
2025-08-11 01:07:53,137 - INFO - PARSED JSON:
2025-08-11 01:07:53,137 - INFO - {
  "channel": "subscriptionResponse",
  "data": {
    "method": "subscribe",
    "subscription": {
      "type": "l2Book",
      "coin": "BTC",
      "nSigFigs": null,
      "mantissa": null
    }
  }
}
2025-08-11 01:07:53,137 - INFO - ================================================================================
2025-08-11 01:07:53,137 - INFO - ================================================================================
2025-08-11 01:07:53,137 - INFO - RAW MESSAGE RECEIVED:
2025-08-11 01:07:53,137 - INFO - {"channel":"l2Book","data":{"coin":"BTC","time":1754899670709,"levels":[[{"px":"121366.0","sz":"11.41902","n":24},{"px":"121365.0","sz":"1.53716","n":5},{"px":"121364.0","sz":"0.0413","n":2},{"px":"121363.0","sz":"0.00011","n":1},{"px":"121362.0","sz":"0.87589","n":4},{"px":"121361.0","sz":"0.00011","n":1},{"px":"121360.0","sz":"0.0413","n":2},{"px":"121359.0","sz":"0.76736","n":2},{"px":"121357.0","sz":"0.88226","n":3},{"px":"121356.0","sz":"0.121","n":1},{"px":"121355.0","sz":"1.28339","n":3},{"px":"121354.0","sz":"1.14205","n":3},{"px":"121353.0","sz":"0.10632","n":2},{"px":"121352.0","sz":"2.24937","n":3},{"px":"121351.0","sz":"0.00011","n":1},{"px":"121350.0","sz":"1.74966","n":7},{"px":"121349.0","sz":"0.21004","n":4},{"px":"121348.0","sz":"0.18781","n":7},{"px":"121347.0","sz":"1.0826","n":4},{"px":"121346.0","sz":"0.91612","n":9}],[{"px":"121367.0","sz":"0.79192","n":3},{"px":"121368.0","sz":"0.00835","n":2},{"px":"121369.0","sz":"0.18428","n":3},{"px":"121370.0","sz":"0.17687","n":2},{"px":"121371.0","sz":"0.00011","n":1},{"px":"121372.0","sz":"0.00011","n":1},{"px":"121373.0","sz":"0.07629","n":4},{"px":"121374.0","sz":"0.00031","n":2},{"px":"121375.0","sz":"1.58295","n":2},{"px":"121376.0","sz":"0.49449","n":2},{"px":"121377.0","sz":"0.00011","n":1},{"px":"121378.0","sz":"0.80615","n":2},{"px":"121379.0","sz":"0.00011","n":1},{"px":"121380.0","sz":"0.49448","n":2},{"px":"121381.0","sz":"0.61052","n":2},{"px":"121382.0","sz":"3.31816","n":4},{"px":"121383.0","sz":"1.04415","n":3},{"px":"121384.0","sz":"0.74087","n":2},{"px":"121385.0","sz":"0.70529","n":6},{"px":"121386.0","sz":"3.84976","n":4}]]}}
2025-08-11 01:07:53,137 - INFO - ================================================================================
2025-08-11 01:07:53,137 - INFO - PARSED JSON:
2025-08-11 01:07:53,138 - INFO - {
  "channel": "l2Book",
  "data": {
    "coin": "BTC",
    "time": 1754899670709,
    "levels": [
      [
        {
          "px": "121366.0",
          "sz": "11.41902",
          "n": 24
        },
        {
          "px": "121365.0",
          "sz": "1.53716",
          "n": 5
        },
        {
          "px": "121364.0",
          "sz": "0.0413",
          "n": 2
        },
        {
          "px": "121363.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121362.0",
          "sz": "0.87589",
          "n": 4
        },
        {
          "px": "121361.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121360.0",
          "sz": "0.0413",
          "n": 2
        },
        {
          "px": "121359.0",
          "sz": "0.76736",
          "n": 2
        },
        {
          "px": "121357.0",
          "sz": "0.88226",
          "n": 3
        },
        {
          "px": "121356.0",
          "sz": "0.121",
          "n": 1
        },
        {
          "px": "121355.0",
          "sz": "1.28339",
          "n": 3
        },
        {
          "px": "121354.0",
          "sz": "1.14205",
          "n": 3
        },
        {
          "px": "121353.0",
          "sz": "0.10632",
          "n": 2
        },
        {
          "px": "121352.0",
          "sz": "2.24937",
          "n": 3
        },
        {
          "px": "121351.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121350.0",
          "sz": "1.74966",
          "n": 7
        },
        {
          "px": "121349.0",
          "sz": "0.21004",
          "n": 4
        },
        {
          "px": "121348.0",
          "sz": "0.18781",
          "n": 7
        },
        {
          "px": "121347.0",
          "sz": "1.0826",
          "n": 4
        },
        {
          "px": "121346.0",
          "sz": "0.91612",
          "n": 9
        }
      ],
      [
        {
          "px": "121367.0",
          "sz": "0.79192",
          "n": 3
        },
        {
          "px": "121368.0",
          "sz": "0.00835",
          "n": 2
        },
        {
          "px": "121369.0",
          "sz": "0.18428",
          "n": 3
        },
        {
          "px": "121370.0",
          "sz": "0.17687",
          "n": 2
        },
        {
          "px": "121371.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121372.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121373.0",
          "sz": "0.07629",
          "n": 4
        },
        {
          "px": "121374.0",
          "sz": "0.00031",
          "n": 2
        },
        {
          "px": "121375.0",
          "sz": "1.58295",
          "n": 2
        },
        {
          "px": "121376.0",
          "sz": "0.49449",
          "n": 2
        },
        {
          "px": "121377.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121378.0",
          "sz": "0.80615",
          "n": 2
        },
        {
          "px": "121379.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121380.0",
          "sz": "0.49448",
          "n": 2
        },
        {
          "px": "121381.0",
          "sz": "0.61052",
          "n": 2
        },
        {
          "px": "121382.0",
          "sz": "3.31816",
          "n": 4
        },
        {
          "px": "121383.0",
          "sz": "1.04415",
          "n": 3
        },
        {
          "px": "121384.0",
          "sz": "0.74087",
          "n": 2
        },
        {
          "px": "121385.0",
          "sz": "0.70529",
          "n": 6
        },
        {
          "px": "121386.0",
          "sz": "3.84976",
          "n": 4
        }
      ]
    ]
  }
}
2025-08-11 01:07:53,138 - INFO - L2BOOK TRANSFORM SUMMARY: {
  "ok": true,
  "pair": "BTC",
  "num_bids": 20,
  "num_asks": 20,
  "best_bid": "121366.0",
  "best_ask": "121367.0",
  "ts_ms": 1754899670709
}
2025-08-11 01:07:53,138 - INFO - L2Book transform verified; stopping test early.
2025-08-11 01:07:53,138 - DEBUG - > CLOSE 1000 (OK) [2 bytes]
2025-08-11 01:07:53,138 - DEBUG - = connection is CLOSING
2025-
"""