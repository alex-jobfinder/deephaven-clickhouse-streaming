# !hyperliquid.py
# SEE https://github.com/search?q=repo%3Aalex-jobfinder%2Fcryptofeed-private%20hyperliquid&type=code
# 3 diff new files.
from cryptofeed.symbols import Symbol
from cryptofeed.util.time import timedelta_str_to_sec
import logging
from typing import Dict, List, Tuple, Union, Optional
from decimal import Decimal

from yapic import json
import asyncio
from cryptofeed.symbols import Symbol, Symbols
from cryptofeed.connection import AsyncConnection, WebsocketEndpoint
from cryptofeed.defines import BUY, SELL, TRADES, L2_BOOK, HYPERLIQUID, CANDLES, FILLS, POSITIONS, BALANCES, ORDER_INFO
from cryptofeed.feed import Feed
from cryptofeed.types import OrderBook, Trade, Candle
from cryptofeed.exceptions import UnsupportedDataFeed

Strings = Union[str, List[str]]
LOG = logging.getLogger('feedhandler')
# from rich import print

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
        CANDLES: 'candle',
    }

    rest_channels = {
        POSITIONS: 'clearinghouseState'  # This is a REST-only channel
    }
    _position_refresh_interval_sec: int = 60  # default every 15 seconds
    
    symbol_endpoint = 'https://api.hyperliquid.xyz/info'

    @property
    def subscription_type(self):
        """Override the subscription type to handle REST channels"""
        return "normalized"

    @property
    def authentication_required(self) -> bool:
        """Override to indicate that authentication is optional"""
        return False

    def requires_authentication(self, subscription):
        """
        Check if any of the subscribed channels require authentication.
        POSITIONS channel is a public REST endpoint and does not require auth.
        """
        LOG.debug(f"[HyperLiquid] requires_authentication check for subscription: {subscription}")
        LOG.debug(f"[HyperLiquid] websocket_channels: {self.websocket_channels}")
        LOG.debug(f"[HyperLiquid] rest_channels: {self.rest_channels}")
        
        # If subscription is empty or None, no auth needed
        if not subscription:
            LOG.debug("[HyperLiquid] Empty subscription, no auth needed")
            return False
            
        # Check each channel in the subscription
        for channel in subscription:
            LOG.debug(f"[HyperLiquid] Checking channel: {channel}")
            # Explicitly mark POSITIONS as not requiring auth
            if channel == POSITIONS:
                LOG.debug("[HyperLiquid] Found POSITIONS channel, skipping auth check")
                continue
            # If we find any channel that's not in our known channels, assume it needs auth
            if channel not in self.websocket_channels and channel not in self.rest_channels:
                LOG.debug(f"[HyperLiquid] Channel {channel} not found in websocket or rest channels, requires auth")
                return True
                
        LOG.debug("[HyperLiquid] No channels requiring auth found")
        return False

    @classmethod
    def _check_no_auth_needed(cls, channel):
        """Helper method to check if a channel needs no authentication"""
        return channel == POSITIONS

    async def _positions_loop(self):
        addresses = self.subscription.get(POSITIONS, [])
        if not addresses:
            return

        LOG.info(f"Starting periodic position fetch loop: {addresses}")
        while True:
            for address in addresses:
                await self._positions(address)
            await asyncio.sleep(self._position_refresh_interval_sec)

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

    # # referenced from coinbase, but removed
    # # def __init__(self, callbacks=None, **kwargs):
    # #     super().__init__(callbacks=callbacks, **kwargs)
    # #     # we only keep track of the L3 order book if we have at least one subscribed order-book callback.
    # #     # use case: subscribing to the L3 book plus Trade type gives you order_type information (see _received below),
    # #     # and we don't need to do the rest of the book-keeping unless we have an active callback
    # #     self.keep_l3_book = False
    # #     if callbacks and L3_BOOK in callbacks:
    # #         self.keep_l3_book = True
    # #     self.__reset()
    # def __init__(self, *args, positions_interval_sec=15, **kwargs):
    #     super().__init__(*args, **kwargs)
    #     self._position_refresh_interval_sec = positions_interval_sec

    def __init__(self, *args, **kwargs):
        # Filter out FILLS from subscription if present since we don't support it yet
        subscription = kwargs.get("subscription", {})
        if FILLS in subscription:
            LOG.warning("FILLS requires auth - removing from subscription")
            del subscription[FILLS]
        kwargs["subscription"] = subscription
        super().__init__(*args, **kwargs)
        self._background_tasks = set()  # Initialize background tasks set

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()

        for chan in list(self.websocket_channels.keys()) + list(self.rest_channels.keys()):
            if chan == POSITIONS:
                LOG.info("Setting up positions polling loop")
                self._background_tasks.add(self._positions_loop())
                continue

            for pair in self.subscription.get(chan, []):
                symbol = self.std_symbol_to_exchange_symbol(pair)
                LOG.debug(f"Subscribing: channel={chan}, symbol={symbol}")

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
                elif chan in self.websocket_channels:
                    sub_msg = {
                        "method": "subscribe",
                        "subscription": {
                            "type": self.websocket_channels[chan],
                            "coin": symbol
                        }
                    }
                    LOG.debug(f"Sending subscribe message: {sub_msg}")
                    await conn.write(json.dumps(sub_msg))


    async def _positions(self, address: str):
        try:
            body = {
                "type": "clearinghouseState",
                "user": address,
            }
            LOG.debug(f"Fetching clearinghouseState for {address}")
            result = await self.http_async.write(
                self.symbol_endpoint,
                data=body,
                is_data_json=True,
                json=True
            )

            # Get asset positions array
            asset_positions = result.get("assetPositions", [])
            for ap in asset_positions:
                # Extract position data
                p = ap.get("position", {})
                
                # Get leverage info
                leverage = p.get("leverage", {})
                leverage_value = leverage.get("value", 0)
                
                # Parse position data
                parsed = {
                    "ts": self.timestamp(),
                    "user": address,
                    "coin": p.get("coin"),
                    "entry_px": p.get("entryPx"),
                    "size": p.get("szi"),  # szi is the size field
                    "unrealized_pnl": p.get("unrealizedPnl"),
                    "leverage": leverage_value,
                    "liq_px": p.get("liquidationPx")
                }
                
                # Only send callback if we have valid coin and size
                if parsed["coin"] and parsed["size"]:
                    await self.callback(POSITIONS, parsed, self.timestamp())
                else:
                    LOG.warning(f"Skipping invalid position data: {parsed}")
                    
        except Exception as e:
            LOG.error(f"Error fetching positions for {address}: {e}", exc_info=True)


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

            users = trade.get("users", [])
            initiator, counterparty = (
                (users[0], users[1]) if side == "B" else (users[1], users[0])
            ) if len(users) >= 2 else ("missing", "missing")

            std_symbol = self.exchange_symbol_to_std_symbol(coin)
            trade_time = trade.get("time", 0)

            enriched_trade = {
                **trade,
                "initiator": initiator,
                "counterparty": counterparty,
            }

            t = Trade(
                self.id,
                std_symbol,
                BUY if side == "B" else SELL,
                Decimal(size),
                Decimal(price),
                self.timestamp_normalize(trade_time),
                id=trade.get("hash"),
                raw=enriched_trade,
            )
            await self.callback(TRADES, t, timestamp)



    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)
        channel = msg.get("channel")
        LOG.debug(f"HyperLiquid raw message received: {msg}")

        handlers = {
            "trades": self._trades,
            "l2Book": self._l2_book,
            # "userFills": self._fills,
            # "candle": self._candles,
        }

        if handler := handlers.get(channel):
            await handler(msg, timestamp)
        else:
            LOG.debug(f"HyperLiquid: Unhandled message type {channel}: {msg}")


    @classmethod
    def std_channel_to_exchange(cls, channel: str) -> str:
        """Override to handle both WebSocket and REST channels."""
        if channel in cls.websocket_channels:
            return cls.websocket_channels[channel]
        elif channel in cls.rest_channels:
            return cls.rest_channels[channel]
        raise UnsupportedDataFeed(f'{channel} is not supported on {cls.id}')

    def is_authenticated_channel(self, channel: str) -> bool:
        """
        Specify which channels require authentication.
        POSITIONS is a public REST endpoint and does not require auth.
        """
        LOG.debug(f"[HyperLiquid] Checking if channel requires auth: {channel}")
        # Only FILLS and other user-private channels need auth
        return channel in {FILLS, BALANCES, ORDER_INFO}

    def std_symbol_to_exchange_symbol(self, symbol: str) -> str:
        """Override to handle user addresses for POSITIONS channel"""
        if symbol.startswith('0x'):  # This is a user address
            return symbol
        return super().std_symbol_to_exchange_symbol(symbol)

    def exchange_symbol_to_std_symbol(self, symbol: str) -> str:
        """Override to handle user addresses for POSITIONS channel"""
        if symbol.startswith('0x'):  # This is a user address
            return symbol
        return super().exchange_symbol_to_std_symbol(symbol)






    # async def _userFills(self, msg: dict, timestamp: float):
    #     fills = msg.get("data", {}).get("fills", [])
    #     if not fills:
    #         LOG.warning(f"HyperLiquid: 'userFills' has no fills: {msg}")
    #         return

    #     for fill in fills:
    #         coin = fill.get("coin")
    #         px = Decimal(fill.get("px", 0))
    #         sz = Decimal(fill.get("sz", 0))
    #         side = BUY if fill.get("side") == "B" else SELL
    #         fill_time = self.timestamp_normalize(fill.get("time", 0))
    #         oid = str(fill.get("oid"))
    #         fee = Decimal(fill.get("fee", 0))
    #         closed_pnl = Decimal(fill.get("closedPnl", 0))
    #         direction = fill.get("dir", "")
    #         start_pos = Decimal(fill.get("startPosition", 0))
    #         crossed = fill.get("crossed", False)
    #         hash_ = fill.get("hash")

    #         liquidation = fill.get("liquidation", {})
    #         if liquidation:
    #             liquidated_user = liquidation.get("liquidatedUser")
    #             mark_px = Decimal(liquidation.get("markPx", 0))
    #             method = liquidation.get("method")
    #             LOG.debug(f"Liquidation detected: {liquidated_user}, method={method}, markPx={mark_px}")

    #         # You can dispatch this to a Fill callback or custom user callback
    #         enriched = {
    #             **fill,
    #             "parsed_closedPnl": closed_pnl,
    #             "parsed_startPosition": start_pos,
    #             "parsed_crossed": crossed,
    #         }

    #         # If you have a callback for FILLS, you can define a dummy Fill object
    #         await self.callback(FILLS, enriched, timestamp)

    
    #     # userFills message examples, fills have an optional key 'liquidation'
    #     # if this is present, the fill is a liquidation & the nested keys should be used as columns.
    #     # complete the rest of the code
    #     """
    #     {
    #         'coin': 'BTC',
    #         'px': '105465.0',
    #         'sz': '0.07668',
    #         'side': 'A',
    #         'time': 1749444493677,
    #         'startPosition': '0.0',
    #         'dir': 'Open Short',
    #         'closedPnl': '0.0',
    #         'hash': '0xa26cc9fdb12588e08775042521fa4101d00097a46d9f1c5875d8d77019912ea7',
    #         'oid': 100954729947,
    #         'crossed': True,
    #         'fee': '2.102634',
    #         'tid': 683786646517791,
    #         'feeToken': 'USDC'
    #     },
    #     {
    #         'coin': 'BTC',
    #         'px': '106890.0',
    #         'sz': '0.03046',
    #         'side': 'B',
    #         'time': 1749462399056,
    #         'startPosition': '-0.07668',
    #         'dir': 'Close Short',
    #         'closedPnl': '-43.4055',
    #         'hash': '0xed38e5ddd6355c4ca8b90425257bd40206f900319b025094426899d4162b079d',
    #         'oid': 100997705114,
    #         'crossed': True,
    #         'fee': '0.846526',
    #         'tid': 1027958770284154,
    #         'liquidation': {'liquidatedUser': '0x5078c2fbea2b2ad61bc840bc023e35fce56bedb6', 'markPx': '106834.0', 'method': 'market'},
    #         'feeToken': 'USDC'
    #     },    
    #     """
