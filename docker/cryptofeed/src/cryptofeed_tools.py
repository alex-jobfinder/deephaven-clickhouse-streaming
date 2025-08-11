# !cryptofeed_tools.py
"""
Orderbook data flow: cryptofeed -> Kafka (ClickHouse)

Context
  - Upstream (exchange-specific feed) emits cryptofeed OrderBook objects via
    `book_callback(L2_BOOK, order_book, ...)`.
  - cryptofeed's `OrderBook.to_dict()` produces a dictionary with:
       {
         'exchange': str,
         'symbol': str,
         'timestamp': float,               # seconds
         'receipt_timestamp': float,       # seconds
         'book': {
           'bid': { Decimal(price): Decimal(size), ... },
           'ask': { Decimal(price): Decimal(size), ... }
         },
         'delta': {...}  # may be present depending on source
       }

This module's responsibilities
  - KafkaCallback: common adapter that receives either dataclass-like objects
    (with `to_dict`) or already-materialized dict payloads and ships them to Kafka.
  - ClickHouseBookKafka: transforms the standardized `book` structure into the
    ClickHouse-friendly layout before producing:
       - Coerces timestamps to nanoseconds: `ts`, `receipt_ts` (int)
       - Extracts and sorts `book['bid']` (desc) and `book['ask']` (asc) into
         OrderedDicts, then moves them to top-level `bid` and `ask` fields
       - Drops the original `book` container (and optional `delta`)

Important
  - If the upstream feed does not populate the canonical `book['bid']` and
    `book['ask']` (for example if it mistakenly uses custom keys or keeps raw
    websocket shapes), this writer will not find the expected structure and
    cannot serialize orderbooks. Ensure the exchange feed sets `book[BID]` and
    `book[ASK]` before invoking the L2 callback.
"""
import asyncio
from collections import OrderedDict

import orjson
from aiokafka import AIOKafkaProducer

SYMBOLS = ['BTC-USD', 'ETH-USD', 'AVAX-USD', 'SOL-USD']
SYMBOLS_HYPERLIQUID = ["ADA", "APT", "ATOM", "AVAX", "BNB", "BTC", "DOGE", "DOT", "ETH", "FARTCOIN", "HYPE", "NEAR", "SOL", "SUI", "TIA", "XRP"]
SYMBOLS = ['BTC-USD']
SYMBOLS_HYPERLIQUID = ["BTC"]

async def my_print(data, _receipt_time):
    print(data)


class KafkaCallback:
    def __init__(self, bootstrap='127.0.0.1', port=9092, topic=None, numeric_type=float, none_to=None,
                 **kwargs):  # working locally
        """
        bootstrap: str, list
            if a list, should be a list of strings in the format: ip/host:port, i.e.
                192.1.1.1:9092
                192.1.1.2:9092
                etc
            if a string, should be ip/port only
        """
        self.bootstrap = bootstrap
        self.port = port
        self.producer = None
        self.topic = topic if topic else self.default_topic
        self.numeric_type = numeric_type
        self.none_to = none_to

    async def __call__(self, dtype, receipt_timestamp: float, **kwargs):  # <-- accept **kwargs
        print(f"DEBUG: KafkaCallback.__call__ dtype={type(dtype)} receipt_ts={receipt_timestamp} kwargs={list(kwargs.keys())}")
        if isinstance(dtype, dict):
            data = dict(dtype)
            print(f"DEBUG: Data is dict with keys: {list(data.keys())}")
        else:
            print(f"DEBUG: Converting dtype to dict, dtype type: {type(dtype)}")
            data = dtype.to_dict(numeric_type=self.numeric_type, none_to=self.none_to)
            # Prefer the explicit 'timestamp' from kwargs if provided
            if 'timestamp' in kwargs:
                data['timestamp'] = kwargs['timestamp']
            elif not getattr(dtype, 'timestamp', None):
                data['timestamp'] = receipt_timestamp
            data['receipt_timestamp'] = receipt_timestamp
            # Optionally carry through raw for debugging
            if 'raw' in kwargs:
                data['raw'] = kwargs['raw']
            print(f"DEBUG: Converted data keys: {list(data.keys())}")
        await self.write(data)

    async def __connect(self):
        if not self.producer:
            loop = asyncio.get_event_loop()
            self.producer = AIOKafkaProducer(acks=0,
                                             loop=loop,
                                             bootstrap_servers=f'{self.bootstrap}:{self.port}' if isinstance(self.bootstrap, str) else self.bootstrap,
                                             client_id='cryptofeed')
            await self.producer.start()

    async def write(self, data: dict):
        await self.__connect()
        await self.producer.send_and_wait(self.topic, orjson.dumps(data).encode('utf-8'))


class ClickHouseTradeKafka(KafkaCallback):
    default_topic = 'trades'

    async def write(self, data: dict):
        await self._KafkaCallback__connect()
        try:
            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            data['size'] = data.pop('amount')
            data['trade_id'] = data.pop('id')
            del data['type']
            await self.producer.send_and_wait(self.topic, orjson.dumps(data))  # orjson uses UTF-8 encoding by default
        except:
            print("WARNING: ClickHouseTradeKafka.write() didn't fire - go check!")
            pass


class ClickHouseBookKafka(KafkaCallback):
    default_topic = 'orderbooks'

    async def write(self, data: dict):
        await self._KafkaCallback__connect()
        try:
            # Expected input: dict from cryptofeed OrderBook.to_dict()
            # Must contain top-level 'book': {'bid': {...}, 'ask': {...}}
            print(f"DEBUG: ClickHouseBookKafka.write() received data keys: {list(data.keys())}")
            if 'book' in data:
                bk = data['book']
                print(f"DEBUG: Book structure keys: {list(bk.keys()) if isinstance(bk, dict) else bk}")
                if isinstance(bk, dict):
                    if 'bid' in bk: print(f"DEBUG: Bid first few: {list(bk['bid'].items())[:3]}")
                    if 'ask' in bk: print(f"DEBUG: Ask first few: {list(bk['ask'].items())[:3]}")

            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            from collections import OrderedDict
            # Sort bids descending (best bid first), asks ascending (best ask first)
            data['bid'] = OrderedDict(sorted(data['book'].pop('bid').items(), reverse=True))
            data['ask'] = OrderedDict(sorted(data['book'].pop('ask').items()))
            del data['book']
            data.pop('delta', None)  # <-- avoid KeyError if not present
            await self.producer.send_and_wait(self.topic, orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS))
        except Exception as e:
            print(f"WARNING: ClickHouseBookKafka.write() failed with error: {e}")
            print(f"DEBUG: Data that caused failure: {data}")
            pass
