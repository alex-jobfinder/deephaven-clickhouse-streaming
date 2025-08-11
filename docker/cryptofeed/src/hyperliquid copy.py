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
        data = msg.get("data")
        if not data:
            LOG.warning(f"HyperLiquid: 'l2Book' message has no data: {msg}")
            return
            
        coin = data.get("coin")
        if not coin:
            LOG.warning(f"HyperLiquid: 'l2Book' message missing coin: {msg}")
            return
            
        std_symbol = self.exchange_symbol_to_std_symbol(coin)
        LOG.debug(f"HyperLiquid: Processing L2 book for exchange symbol '{coin}' -> std symbol '{std_symbol}'")
        
        # Get the levels array: [bids, asks]
        levels = data.get("levels", [])
        if len(levels) != 2:
            LOG.warning(f"HyperLiquid: Expected 2 levels arrays (bids, asks), got {len(levels)}: {msg}")
            return
            
        bids_data, asks_data = levels
        book_timestamp = self.timestamp_normalize(data.get("time", 0))
        
        # Initialize order book if not exists
        if std_symbol not in self._l2_book:
            self._l2_book[std_symbol] = OrderBook(
                self.id,
                std_symbol,
                max_depth=self.max_depth
            )
        
        book = self._l2_book[std_symbol]
        
        # Clear existing book data for full snapshot
        book.book.bids.clear()
        book.book.asks.clear()
        
        # Process bids (buyers) - should be sorted highest to lowest
        for bid_level in bids_data:
            price = Decimal(bid_level.get("px", "0"))
            size = Decimal(bid_level.get("sz", "0"))
            
            if price > 0 and size > 0:
                book.book.bids[price] = size
        
        # Process asks (sellers) - should be sorted lowest to highest  
        for ask_level in asks_data:
            price = Decimal(ask_level.get("px", "0"))
            size = Decimal(ask_level.get("sz", "0"))
            
            if price > 0 and size > 0:
                book.book.asks[price] = size
        
        # Update book timestamp
        book.timestamp = book_timestamp
        
        LOG.debug(f"HyperLiquid: Updated L2 book for {std_symbol} - "
                 f"Bids: {len(book.book.bids)}, Asks: {len(book.book.asks)}, "
                 f"Best bid: {max(book.book.bids.keys()) if book.book.bids else 'N/A'}, "
                 f"Best ask: {min(book.book.asks.keys()) if book.book.asks else 'N/A'}")
        
        # Send callback with the updated order book
        await self.callback(L2_BOOK, book, timestamp)
        LOG.debug(f"HyperLiquid: L2_BOOK callback executed for {std_symbol}")

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


