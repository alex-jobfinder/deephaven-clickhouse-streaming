# !hyperliquid.py
from cryptofeed.symbols import Symbol
from cryptofeed.util.time import timedelta_str_to_sec
import logging
from typing import Dict, List, Tuple
from decimal import Decimal

from yapic import json

from cryptofeed.symbols import Symbol, Symbols
from cryptofeed.connection import AsyncConnection, WebsocketEndpoint
from cryptofeed.defines import BUY, SELL, TRADES, L2_BOOK, HYPERLIQUID, CANDLES
from cryptofeed.feed import Feed
from cryptofeed.types import OrderBook, Trade, Candle

LOG = logging.getLogger('feedhandler')


class HyperLiquid(Feed):
    id = HYPERLIQUID
    websocket_endpoints = [
        WebsocketEndpoint('wss://api.hyperliquid.xyz/ws', options={'compression': None}),
        WebsocketEndpoint('wss://api.hyperliquid-testnet.xyz/ws', options={'compression': None})
    ]
    rest_endpoints = []
    valid_candle_intervals = {'1m', '5m', '15m', '1h'}
    candle_interval_map = {k: k for k in valid_candle_intervals}
    websocket_channels = {
        L2_BOOK: 'l2Book',
        TRADES: 'trades',
        CANDLES: 'candle'
    }
    symbol_endpoint = 'https://api.hyperliquid.xyz/info'

    @classmethod
    def timestamp_normalize(cls, ts: float | int) -> Decimal:
        return Decimal(str(ts)) / Decimal("1000.0")

    @classmethod
    def symbol_mapping(cls, refresh=False):
        if Symbols.populated(cls.id) and not refresh:
            return Symbols.get(cls.id)[0]

        try:
            response = cls.http_sync.write(
                cls.symbol_endpoint,
                data={"type": "allMids"},
                is_data_json=True,
                json=True
            )

            syms, info = cls._parse_symbol_data(response)
            LOG.debug(f"HyperLiquid symbol mapping result: {syms}")
            Symbols.set(cls.id, syms, info)
            return syms
        except Exception as e:
            LOG.error("%s: Failed to parse symbol information: %s", cls.id, str(e), exc_info=True)
            raise

    @classmethod
    def _parse_symbol_data(cls, data: dict = None) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        symbols_map: Dict[str, str] = {}
        info: Dict[str, Dict[str, str]] = {'instrument_type': {}}

        if not data:
            LOG.error("HyperLiquid info endpoint returned no data or invalid response")
            return symbols_map, info

        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception as e:
                LOG.error(f"Failed to parse JSON from HyperLiquid response: {e}")
                return symbols_map, info

        if not isinstance(data, dict):
            LOG.error("Unexpected format from HyperLiquid info endpoint (expected dict of symbols)")
            return symbols_map, info

        for sym in data.keys():
            if sym.startswith('@'):
                continue
            symbols_map[sym] = sym
            info['instrument_type'][sym] = 'perp'

        return symbols_map, info

    def __reset(self):
        self._l2_book: Dict[str, OrderBook] = {}

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()
        for chan in self.websocket_channels:
            for pair in self.subscription.get(chan, []):
                symbol = self.std_symbol_to_exchange_symbol(pair)
                LOG.debug(f"HyperLiquid subscribe: channel={chan}, std_symbol={pair}, exchange_symbol={symbol}")

                if chan == CANDLES:
                    for interval in self.subscription_interval[chan][pair]:
                        sub_msg = {
                            "method": "subscribe",
                            "subscription": {
                                "type": self.websocket_channels[chan],
                                "coin": symbol,
                                "interval": interval
                            }
                        }
                        LOG.debug(f"Sending candle subscribe message: {sub_msg}")
                        await conn.write(json.dumps(sub_msg))
                else:
                    sub_msg = {
                        "method": "subscribe",
                        "subscription": {
                            "type": self.websocket_channels[chan],
                            "coin": symbol
                        }
                    }
                    LOG.debug(f"Sending subscribe message: {sub_msg}")
                    await conn.write(json.dumps(sub_msg))


    async def _handle_l2_book(self, msg: dict, timestamp: float):
        """Handle L2 book (order book) messages from HyperLiquid"""
        try:
            data = msg.get("data", {})
            
            # Skip if this is just a subscription acknowledgment snapshot
            if data.get("isSnapshot"):
                LOG.debug(f"HyperLiquid: Skipping subscription snapshot for {data.get('coin', 'unknown')}")
                return
            coin = data.get("coin")
            
            if not coin:
                LOG.warning(f"HyperLiquid: L2 book message missing coin: {msg}")
                return
                
            std_symbol = self.exchange_symbol_to_std_symbol(coin)
            
            # Get the levels data - it's a list with [bids, asks]
            levels = data.get("levels", [])
            
            if len(levels) != 2:
                LOG.warning(f"HyperLiquid: Unexpected levels format for {coin}: {levels}")
                return
                
            bids_data, asks_data = levels[0], levels[1]
            
            # Initialize order book if it doesn't exist
            if std_symbol not in self._l2_book:
                self._l2_book[std_symbol] = OrderBook(self.id, std_symbol)
                
            ob = self._l2_book[std_symbol]
            
            # Clear the book (HyperLiquid sends full snapshots)
            ob.book.bids.clear()
            ob.book.asks.clear()
            
            # Process bids
            for level in bids_data:
                price = Decimal(level["px"])
                size = Decimal(level["sz"])
                if size > 0:  # Only add non-zero sizes
                    ob.book.bids[price] = size
                    
            # Process asks  
            for level in asks_data:
                price = Decimal(level["px"])
                size = Decimal(level["sz"])
                if size > 0:  # Only add non-zero sizes
                    ob.book.asks[price] = size
            
            # Update timestamp from the message
            book_time = data.get("time", 0)
            if book_time:
                ob.timestamp = self.timestamp_normalize(book_time)
            else:
                ob.timestamp = timestamp
                
            LOG.debug(f"HyperLiquid: Updated L2 book for {std_symbol} - "
                    f"bids: {len(ob.book.bids)}, asks: {len(ob.book.asks)}")
            
            # Send the callback
            await self.callback(L2_BOOK, ob, timestamp)
            LOG.debug(f"HyperLiquid: L2_BOOK callback executed for {std_symbol}")
            
        except Exception as e:
            LOG.error(f"HyperLiquid: Error processing L2 book message: {e}", exc_info=True)
            LOG.error(f"HyperLiquid: Problematic message: {msg}")

    async def message_handler(self, msg: str, conn, timestamp: float):
        LOG.debug(f"HyperLiquid raw message received: {msg}")
        msg = json.loads(msg, parse_float=Decimal)

        if msg.get("channel") == "trades":
            data = msg.get("data", [])
            if not data:
                LOG.warning(f"HyperLiquid: 'trades' message has no data: {msg}")
            for trade in data:
                coin = trade.get("coin")
                side = trade.get("side")
                price = trade.get("px")
                size = trade.get("sz")
                LOG.debug(f"Parsing trade: coin={coin}, side={side}, price={price}, size={size}, raw={trade}")

                users = trade.get("users", [])

                if not isinstance(users, list) or len(users) < 2:
                    initiator = "missing"
                    counterparty = "missing"
                else:
                    if side == "B":
                        initiator = users[0]
                        counterparty = users[1]
                    else:
                        initiator = users[1]
                        counterparty = users[0]

                std_symbol = self.exchange_symbol_to_std_symbol(coin)
                LOG.debug(f"HyperLiquid: Mapped exchange symbol '{coin}' to std symbol '{std_symbol}'")

                trade_time = trade.get("time", 0)
                enriched_trade = {
                    **trade,
                    "initiator": initiator,
                    "counterparty": counterparty
                }

                t = Trade(
                    self.id,
                    std_symbol,
                    BUY if side == "B" else SELL,
                    Decimal(size),
                    Decimal(price),
                    self.timestamp_normalize(trade_time),
                    id=trade.get("hash"),
                    raw=enriched_trade
                )
                LOG.debug(f"HyperLiquid: Constructed Trade event: {t}")
                await self.callback(TRADES, t, timestamp)
                LOG.debug(f"HyperLiquid: TRADES callback executed for {std_symbol}")

        elif msg.get("channel") == "l2Book":
            await self._handle_l2_book(msg, timestamp)
        
        else:
            LOG.debug(f"HyperLiquid: Unhandled message type {msg.get('channel')}: {msg}")

"""
The subscription ack provides a snapshot of previous data for time series data (e.g. user fills). These snapshot messages are tagged with isSnapshot: true and can be ignored if the previous messages were already processed.

l2Book:

    Subscription message: { "type": "l2Book", "coin": "<coin_symbol>" }

    Optional parameters: nSigFigs: int, mantissa: int

    Data format: WsBook
    
// Snapshot feed, pushed on each block that is at least 0.5 since last push
interface WsBook {
  coin: string;
  levels: [Array<WsLevel>, Array<WsLevel>];
  time: number;
}    


Connecting to wss://api.hyperliquid.xyz/ws
SENDING:
{"method": "subscribe", "subscription": {"type": "l2Book", "coin": "BTC"}}
RECEIVED (raw):
{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":null,"mantissa":null}}}
RECEIVED (raw):
{"channel":"l2Book","data":{"coin":"BTC","time":1754880820294,"levels":[[{"px":"121902.0","sz":"9.43303","n":32},{"px":"121901.0","sz":"0.87951","n":4},{"px":"121900.0","sz":"0.06761","n":5},{"px":"121899.0","sz":"0.00041","n":1},{"px":"121898.0","sz":"0.04142","n":2},{"px":"121897.0","sz":"3.05609","n":3},{"px":"121896.0","sz":"0.4286","n":2},{"px":"121894.0","sz":"0.00011","n":1},{"px":"121893.0","sz":"0.00011","n":1},{"px":"121892.0","sz":"0.00052","n":2},{"px":"121891.0","sz":"0.00011","n":1},{"px":"121890.0","sz":"1.21544","n":6},{"px":"121889.0","sz":"7.95758","n":5},{"px":"121888.0","sz":"3.28839","n":7},{"px":"121887.0","sz":"1.51249","n":5},{"px":"121886.0","sz":"1.32745","n":8},{"px":"121885.0","sz":"2.24706","n":5},{"px":"121884.0","sz":"0.78159","n":5},{"px":"121883.0","sz":"2.41742","n":3},{"px":"121882.0","sz":"6.84213","n":7}],[{"px":"121903.0","sz":"3.4821","n":6},{"px":"121904.0","sz":"0.44456","n":2},{"px":"121905.0","sz":"0.00011","n":1},{"px":"121906.0","sz":"1.24028","n":3},{"px":"121907.0","sz":"0.00011","n":1},{"px":"121908.0","sz":"0.00011","n":1},{"px":"121909.0","sz":"0.52916","n":3},{"px":"121910.0","sz":"2.30332","n":4},{"px":"121911.0","sz":"3.26097","n":3},{"px":"121912.0","sz":"4.92375","n":2},{"px":"121913.0","sz":"0.60289","n":3},{"px":"121914.0","sz":"0.41522","n":3},{"px":"121915.0","sz":"3.0846","n":6},{"px":"121916.0","sz":"0.00109","n":2},{"px":"121917.0","sz":"0.71634","n":7},{"px":"121918.0","sz":"0.59808","n":4},{"px":"121919.0","sz":"1.02673","n":5},{"px":"121920.0","sz":"3.43697","n":5},{"px":"121921.0","sz":"4.32618","n":5},{"px":"121922.0","sz":"0.01011","n":2}]]}}
RECEIVED (parsed pretty):
{
  "channel": "l2Book",
  "data": {
    "coin": "BTC",
    "time": 1754880820294,
    "levels": [
      [
        {
          "px": "121902.0",
          "sz": "9.43303",
          "n": 32
        },
        {
          "px": "121901.0",
          "sz": "0.87951",
          "n": 4
        },
        {
          "px": "121900.0",
          "sz": "0.06761",
          "n": 5
        },
        {
          "px": "121899.0",
          "sz": "0.00041",
          "n": 1
        },
        {
          "px": "121898.0",
          "sz": "0.04142",
          "n": 2
        },
        {
          "px": "121897.0",
          "sz": "3.05609",
          "n": 3
        },
        {
          "px": "121896.0",
          "sz": "0.4286",
          "n": 2
        },
        {
          "px": "121894.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121893.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121892.0",
          "sz": "0.00052",
          "n": 2
        },
        {
          "px": "121891.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121890.0",
          "sz": "1.21544",
          "n": 6
        },
        {
          "px": "121889.0",
          "sz": "7.95758",
          "n": 5
        },
        {
          "px": "121888.0",
          "sz": "3.28839",
          "n": 7
        },
        {
          "px": "121887.0",
          "sz": "1.51249",
          "n": 5
        },
        {
          "px": "121886.0",
          "sz": "1.32745",
          "n": 8
        },
        {
          "px": "121885.0",
          "sz": "2.24706",
          "n": 5
        },
        {
          "px": "121884.0",
          "sz": "0.78159",
          "n": 5
        },
        {
          "px": "121883.0",
          "sz": "2.41742",
          "n": 3
        },
        {
          "px": "121882.0",
          "sz": "6.84213",
          "n": 7
        }
      ],
      [
        {
          "px": "121903.0",
          "sz": "3.4821",
          "n": 6
        },
        {
          "px": "121904.0",
          "sz": "0.44456",
          "n": 2
        },
        {
          "px": "121905.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121906.0",
          "sz": "1.24028",
          "n": 3
        },
        {
          "px": "121907.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121908.0",
          "sz": "0.00011",
          "n": 1
        },
        {
          "px": "121909.0",
          "sz": "0.52916",
          "n": 3
        },
        {
          "px": "121910.0",
          "sz": "2.30332",
          "n": 4
        },
        {
          "px": "121911.0",
          "sz": "3.26097",
          "n": 3
        },
        {
          "px": "121912.0",
          "sz": "4.92375",
          "n": 2
        },
        {
          "px": "121913.0",
          "sz": "0.60289",
          "n": 3
        },
        {
          "px": "121914.0",
          "sz": "0.41522",
          "n": 3
        },
        {
          "px": "121915.0",
          "sz": "3.0846",
          "n": 6
        },
        {
          "px": "121916.0",
          "sz": "0.00109",
          "n": 2
        },
        {
          "px": "121917.0",
          "sz": "0.71634",
          "n": 7
        },
        {
          "px": "121918.0",
          "sz": "0.59808",
          "n": 4
        },
        {
          "px": "121919.0",
          "sz": "1.02673",
          "n": 5
        },
        {
          "px": "121920.0",
          "sz": "3.43697",
          "n": 5
        },
        {
          "px": "121921.0",
          "sz": "4.32618",
          "n": 5
        },
        {
          "px": "121922.0",
          "sz": "0.01011",
          "n": 2
        }
      ]
    ]
  }
}

"""