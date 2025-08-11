# !hyperliquid.py
from cryptofeed.symbols import Symbol
from cryptofeed.util.time import timedelta_str_to_sec
import logging
from typing import Dict, List, Tuple, Optional, Union
from decimal import Decimal

from yapic import json

from cryptofeed.symbols import Symbol, Symbols
from cryptofeed.connection import AsyncConnection, WebsocketEndpoint
from cryptofeed.defines import BUY, SELL, TRADES, L2_BOOK, HYPERLIQUID, CANDLES, BID, ASK
from cryptofeed.feed import Feed
from cryptofeed.types import OrderBook, Trade, Candle

LOG = logging.getLogger('feedhandler')

# @classmethod
# def timestamp_normalize(cls, ts: Union[float, int]) -> Decimal: ...
# def _infer_single_l2_symbol(self) -> Optional[str]: ...

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
        # internal orderbook cache (avoid name collision with l2 handler)
        self._books: Dict[str, OrderBook] = {}

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

    async def _trades(self, msg: dict, timestamp: float):
        data = msg.get("data", [])
        if not data:
            LOG.warning(f"HyperLiquid: 'trades' message has no data: {msg}")
            return

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

    def _infer_single_l2_symbol(self) -> str | None:
        """
        If exactly one std symbol is subscribed to L2, return it; else None.
        Helpful when handling REST-style array snapshots (no coin in payload).
        """
        pairs = self.subscription.get(L2_BOOK) or []
        if isinstance(pairs, list) and len(pairs) == 1:
            return pairs[0]
        return None

    """
    2025-08-10 17:00:44,326 - INFO - ================================================================================
    2025-08-10 17:00:44,326 - INFO - RAW MESSAGE RECEIVED:
    2025-08-10 17:00:44,326 - INFO - {"channel":"l2Book","data":{"coin":"BTC","time":1754870444265,"levels":[[{"px":"119292.0","sz":"7.83756","n":24},{"px":"119291.0","sz":"0.0009","n":1},{"px":"119290.0","sz":"1.64482","n":3},{"px":"119288.0","sz":"0.05029","n":1},{"px":"119286.0","sz":"1.31","n":4},{"px":"119283.0","sz":"0.43488","n":1},{"px":"119282.0","sz":"0.02235","n":1},{"px":"119281.0","sz":"0.13355","n":1},{"px":"119280.0","sz":"0.5383","n":5},{"px":"119279.0","sz":"1.76021","n":3},{"px":"119278.0","sz":"4.71096","n":3},{"px":"119277.0","sz":"0.00011","n":1},{"px":"119276.0","sz":"4.19094","n":3},{"px":"119275.0","sz":"1.32376","n":4},{"px":"119274.0","sz":"0.91875","n":4},{"px":"119273.0","sz":"4.27608","n":2},{"px":"119272.0","sz":"0.441","n":3},{"px":"119271.0","sz":"1.08604","n":4},{"px":"119270.0","sz":"4.64041","n":13},{"px":"119269.0","sz":"0.00011","n":1}],[{"px":"119293.0","sz":"8.82288","n":9},{"px":"119294.0","sz":"0.04405","n":2},{"px":"119296.0","sz":"0.00011","n":1},{"px":"119297.0","sz":"0.00052","n":2},{"px":"119298.0","sz":"0.02846","n":2},{"px":"119299.0","sz":"0.00011","n":1},{"px":"119300.0","sz":"0.61147","n":4},{"px":"119301.0","sz":"0.61159","n":3},{"px":"119302.0","sz":"0.06438","n":5},{"px":"119303.0","sz":"0.00052","n":2},{"px":"119304.0","sz":"0.49558","n":2},{"px":"119305.0","sz":"0.08392","n":2},{"px":"119306.0","sz":"0.43499","n":2},{"px":"119307.0","sz":"0.11933","n":4},{"px":"119308.0","sz":"0.12111","n":2},{"px":"119309.0","sz":"0.51916","n":4},{"px":"119310.0","sz":"1.03221","n":4},{"px":"119311.0","sz":"0.44011","n":2},{"px":"119312.0","sz":"0.84693","n":3},{"px":"119313.0","sz":"14.49836","n":9}]]}}
    2025-08-10 17:00:44,326 - INFO - ================================================================================
    """
    async def _handle_l2_book(self, msg: dict, timestamp: float):
        """
        Handle both:
          - WebSocket WsBook: data = { coin: str, levels: [bids[], asks[]], time: int(ms) }
          - REST /info l2Book snapshot (per l2book.yml): data = [[{px,sz,n}...],[{px,sz,n}...]]
            (no coin, no time)
        """
        raw_data = msg.get("data", None)

        # Normalize into (std_symbol, levels, time_ms)
        std_symbol = None
        time_ms = None
        levels = None

        if isinstance(raw_data, dict) and "levels" in raw_data:
            # WebSocket WsBook
            coin = raw_data.get("coin")
            if not coin:
                LOG.warning(f"HyperLiquid: WsBook missing 'coin': {msg}")
                return
            try:
                std_symbol = self.exchange_symbol_to_std_symbol(coin)
            except Exception:
                LOG.warning(f"HyperLiquid: Unknown symbol mapping for coin={coin}")
                return
            levels = raw_data.get("levels")
            # Fix: Extract timestamp from data.data.time (nested structure)
            time_ms = raw_data.get("time", None)

        elif isinstance(raw_data, list) and len(raw_data) == 2 and all(isinstance(x, list) for x in raw_data):
            # REST-style snapshot arrays
            inferred = self._infer_single_l2_symbol()
            if not inferred:
                LOG.warning("HyperLiquid: array-style L2 snapshot without coin and multiple/no L2 subscriptions; skipping")
                return
            std_symbol = inferred
            levels = raw_data
            time_ms = None  # use receipt-based timestamp below if exchange time not provided

        else:
            LOG.warning(f"HyperLiquid: bad l2Book message shape: {msg}")
            return

        if not isinstance(levels, list) or len(levels) != 2:
            LOG.warning(f"HyperLiquid: malformed 'levels' in l2Book: {msg}")
            return

        bids_raw, asks_raw = levels
        bids, asks = {}, {}

        for lvl in bids_raw or []:
            try:
                px = Decimal(lvl["px"])
                sz = Decimal(lvl["sz"])
                if sz > 0:
                    bids[px] = sz
            except Exception as e:
                LOG.debug(f"HyperLiquid bid parse err {lvl}: {e}")

        for lvl in asks_raw or []:
            try:
                px = Decimal(lvl["px"])
                sz = Decimal(lvl["sz"])
                if sz > 0:
                    asks[px] = sz
            except Exception as e:
                LOG.debug(f"HyperLiquid ask parse err {lvl}: {e}")

        book = self._books.get(std_symbol)
        if book is None:
            book = OrderBook(self.id, std_symbol, max_depth=self.max_depth, raw=msg)
            book.exchange = self.id  # Add this line to ensure exchange is set
            self._books[std_symbol] = book
            LOG.debug(f"HyperLiquid: Created new OrderBook for {std_symbol}, exchange: {book.exchange}")
        else:
            book.raw = msg
            LOG.debug(f"HyperLiquid: Updated existing OrderBook for {std_symbol}, exchange: {book.exchange}")

        # Populate using BID/ASK keys per cryptofeed convention
        book.book[BID] = bids
        book.book[ASK] = asks

        # Fix: Properly set the timestamp from the exchange data
        if time_ms is not None:
            book.timestamp = self.timestamp_normalize(time_ms)
        else:
            # If no exchange timestamp, use the receipt timestamp
            book.timestamp = self.timestamp_normalize(timestamp * 1000)

        LOG.debug(f"HyperLiquid: Processed orderbook for {std_symbol}, bids: {len(bids)}, asks: {len(asks)}, exchange: {book.exchange}")
        LOG.debug(f"HyperLiquid: OrderBook object keys: {list(book.__dict__.keys()) if hasattr(book, '__dict__') else 'No __dict__'}")
        LOG.debug(f"HyperLiquid: OrderBook.book structure: {book.book if hasattr(book, 'book') else 'No book attribute'}")
        
        await self.book_callback(L2_BOOK, book, timestamp)
        LOG.debug(f"HyperLiquid: L2_BOOK callback executed for {std_symbol}")



    async def message_handler(self, msg: str, conn, timestamp: float):
        LOG.debug(f"HyperLiquid raw message received: {msg}")
        try:
            msg = json.loads(msg, parse_float=Decimal)
            channel = msg.get("channel")
            LOG.debug(f"HyperLiquid parsed message - channel: {channel}, timestamp: {timestamp}")

            handlers = {
                "trades": self._trades,
                "l2Book": self._handle_l2_book,
                # "candle": self._handle_candles,  # (optional future)
            }

            handler = handlers.get(channel)
            if handler:
                LOG.debug(f"HyperLiquid calling handler for channel: {channel}")
                await handler(msg, timestamp)
            else:
                LOG.debug(f"HyperLiquid: Unhandled message type {channel}: {msg}")
        except Exception as e:
            LOG.error(f"HyperLiquid message_handler error: {e}", exc_info=True)
