# !hyperliquid.py
from cryptofeed.symbols import Symbol
from cryptofeed.util.time import timedelta_str_to_sec
import logging
from typing import Dict, List, Tuple
from decimal import Decimal

from yapic import json

from cryptofeed.symbols import Symbol, Symbols
from cryptofeed.connection import AsyncConnection, WebsocketEndpoint
from cryptofeed.defines import BUY, SELL, TRADES, L2_BOOK, HYPERLIQUID, CANDLES, BID, ASK
from cryptofeed.feed import Feed
from cryptofeed.types import OrderBook, Trade, Candle

LOG = logging.getLogger('feedhandler')


class HyperLiquid(Feed):
    """
    Data flow overview for L2 order books (WsBook):

    1) Subscription
       - FeedHandler constructs `subscription={L2_BOOK: ["BTC", ...]}`.
       - `subscribe()` maps the standard channel `L2_BOOK` to the exchange channel
         string `'l2Book'` and sends websocket messages of the form:
            {
              "method": "subscribe",
              "subscription": { "type": "l2Book", "coin": "BTC" }
            }

    2) Inbound websocket messages
       - The exchange pushes snapshots on each block (≥ 0.5s since last push):
            {
              "channel": "l2Book",
              "data": {
                "coin": "BTC",
                "levels": [ [ {px, sz, n}, ... ], [ {px, sz, n}, ... ] ],
                "time": <milliseconds>
              }
            }
         where `levels[0]` are bids and `levels[1]` are asks. Prices (px) and sizes (sz)
         are strings; we parse them as Decimal.

    3) Normalization into cryptofeed OrderBook
       - `message_handler()` handles `channel == "l2Book"`:
           a. Map exchange symbol (e.g., 'BTC') to the standard symbol.
           b. Convert `levels` into two dicts of Decimal price -> Decimal size.
           c. Populate the OrderBook's internal structure using the canonical
              keys `BID` and `ASK` (required by cryptofeed):
                 book[BID] = { price: size, ... }
                 book[ASK] = { price: size, ... }
           d. Normalize the exchange timestamp `data.time` (ms) to seconds.

    4) Emitting the callback
       - Call `book_callback(L2_BOOK, order_book, ...)`. Cryptofeed's default
         serialization (`to_dict`) produces a dict with a top-level `book` key:
            {
              'exchange': 'HYPERLIQUID',
              'symbol': 'BTC',
              'timestamp': <float seconds>,
              'receipt_timestamp': <float seconds>,
              'book': {
                'bid': { Decimal(price): Decimal(size), ... },
                'ask': { Decimal(price): Decimal(size), ... }
              },
              'delta': {...}
            }
         Downstream sinks (e.g., ClickHouseBookKafka) expect `book['bid']` and
         `book['ask']` to exist; they will reorder these into arrays for storage.

    Notes:
      - This Feed implementation only handles the websocket WsBook path (no REST
        snapshots). If you pass raw websocket messages directly to a Kafka writer,
        you won't see a `book` key—only the normalized OrderBook via callback has it.
    """
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
            # Use perps metadata (universe names like 'BTC', 'ETH', ...)
            response = cls.http_sync.write(
                cls.symbol_endpoint,
                data={"type": "meta"},
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

        # Expect perps meta shape: { "universe": [ { "name": "BTC", "isDelisted": false }, ... ] }
        if isinstance(data, dict) and 'universe' in data and isinstance(data['universe'], list):
            for entry in data['universe']:
                if not isinstance(entry, dict):
                    continue
                name = entry.get('name')
                if not name or not isinstance(name, str):
                    continue
                # Do not filter out delisted entries; include the full universe as requested
                # Mapping: normalized symbol is same as exchange symbol for perps, e.g. 'BTC'
                symbols_map[name] = name
                # info['instrument_type'][name] = 'perp'
            return symbols_map, info

        LOG.error("Unexpected format from HyperLiquid meta endpoint; could not find perps universe")

        return symbols_map, info

    def __reset(self):
        self._l2_book: Dict[str, OrderBook] = {}

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()
        # Iterate actual subscription keys provided by the FeedHandler
        for chan in self.subscription:
            pairs = self.subscription.get(chan, [])
            # Map standard channel to exchange channel if needed
            exch_chan = self.websocket_channels.get(chan, chan)
            for pair in pairs:
                symbol = self.std_symbol_to_exchange_symbol(pair)
                LOG.debug(f"HyperLiquid subscribe: channel={chan} ({exch_chan}), std_symbol={pair}, exchange_symbol={symbol}")

                if chan == CANDLES or exch_chan == self.websocket_channels.get(CANDLES):
                    for interval in self.subscription_interval[chan][pair]:
                        sub_msg = {
                            "method": "subscribe",
                            "subscription": {
                                "type": exch_chan,
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
                            "type": exch_chan,
                            "coin": symbol
                        }
                    }
                    LOG.debug(f"Sending subscribe message: {sub_msg}")
                    await conn.write(json.dumps(sub_msg))

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
            # WebSocket WsBook only: { coin: string, levels: [bids[], asks[]], time: number }
            data = msg.get("data", {})
            coin = data.get("coin")
            levels = data.get("levels")
            time_ms = data.get("time")

            if not coin or not isinstance(levels, list) or len(levels) != 2 or time_ms is None:
                LOG.warning(f"HyperLiquid: malformed l2Book message: {msg}")
                return

            pair = self.exchange_symbol_to_std_symbol(coin)
            if pair not in self._l2_book:
                self._l2_book[pair] = OrderBook(self.id, pair, max_depth=self.max_depth)

            bids_raw = levels[0]
            asks_raw = levels[1]

            bids = {Decimal(level["px"]): Decimal(level["sz"]) for level in bids_raw}
            asks = {Decimal(level["px"]): Decimal(level["sz"]) for level in asks_raw}

            ob = self._l2_book[pair]
            # Populate using BID/ASK keys per cryptofeed convention
            ob.book[BID] = bids
            ob.book[ASK] = asks
            ob.timestamp = self.timestamp_normalize(time_ms)
            ob.raw = msg

            await self.book_callback(L2_BOOK, ob, timestamp, timestamp=ob.timestamp, raw=msg)

        else:
            LOG.debug(f"HyperLiquid: Unhandled message type {msg.get('channel')}: {msg}")

