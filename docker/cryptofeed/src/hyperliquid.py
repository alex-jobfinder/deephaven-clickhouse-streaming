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
                std_symbol = self.exchange_symbol_to_exchange_symbol(coin)
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
        else:
            book.raw = msg

        # Populate using BID/ASK keys per cryptofeed convention
        book.book[BID] = bids
        book.book[ASK] = asks

        # Fix: Properly set the timestamp from the exchange data
        if time_ms is not None:
            book.timestamp = self.timestamp_normalize(time_ms)
        else:
            # If no exchange timestamp, use the receipt timestamp
            book.timestamp = self.timestamp_normalize(timestamp * 1000)

        await self.book_callback(L2_BOOK, book, timestamp)



    async def message_handler(self, msg: str, conn, timestamp: float):
        LOG.debug(f"HyperLiquid raw message received: {msg}")
        msg = json.loads(msg, parse_float=Decimal)
        channel = msg.get("channel")

        handlers = {
            "trades": self._trades,
            "l2Book": self._handle_l2_book,
            # "candle": self._handle_candles,  # (optional future)
        }

        handler = handlers.get(channel)
        if handler:
            await handler(msg, timestamp)
        else:
            LOG.debug(f"HyperLiquid: Unhandled message type {channel}: {msg}")
