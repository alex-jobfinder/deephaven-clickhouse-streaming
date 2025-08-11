#!/usr/bin/env python3
import asyncio
import json
import argparse
import sys
from websockets import connect


async def fetch_one_l2book_message(coin: str, timeout: float):
    uri = "wss://api.hyperliquid.xyz/ws"
    print(f"Connecting to {uri}")

    try:
        async with connect(uri) as websocket:
            sub_msg = {
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": coin},
            }

            # Show exactly what we send
            print("SENDING:")
            print(json.dumps(sub_msg))
            await websocket.send(json.dumps(sub_msg))

            # Wait for messages until we get the first l2Book for this coin
            while True:
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    print(f"Timeout waiting for l2Book {coin}")
                    return 1

                print("RECEIVED (raw):")
                print(raw)

                # Try to parse JSON
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Only stop when the message is the l2Book for our coin
                if parsed.get("channel") == "l2Book" and parsed.get("data", {}).get("coin") == coin:
                    print("RECEIVED (parsed pretty):")
                    print(json.dumps(parsed, indent=2))
                    return 0
    except Exception as e:
        print(f"Connection error: {e}")
        return 2


def main():
    parser = argparse.ArgumentParser(description="Fetch a single HyperLiquid l2Book message and exit")
    parser.add_argument("--coin", default="BTC", help="Coin symbol, e.g. BTC")
    parser.add_argument("--timeout", type=float, default=10.0, help="Seconds to wait for a message")
    args = parser.parse_args()

    exit_code = asyncio.run(fetch_one_l2book_message(args.coin, args.timeout))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
