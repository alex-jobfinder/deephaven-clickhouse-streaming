import asyncio
from collections import OrderedDict

import orjson
from aiokafka import AIOKafkaProducer
try:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
except Exception:  # older aiokafka fallback
    AIOKafkaAdminClient = None
    NewTopic = None

SYMBOLS = ['BTC-USD', 'ETH-USD', 'AVAX-USD', 'SOL-USD']
SYMBOLS_HYPERLIQUID = ["ADA", "APT", "ATOM", "AVAX", "BNB", "BTC", "DOGE", "DOT", "ETH", "FARTCOIN", "HYPE", "NEAR", "SOL", "SUI", "TIA", "XRP"]
SYMBOLS = ['BTC-USD']
SYMBOLS_HYPERLIQUID = ["BTC"]


async def my_print(data, _receipt_time):
    print(data)


async def print_orderbook_in(orderbook, receipt_timestamp: float):
    """Debug printer for inbound OrderBook events from exchanges."""
    try:
        exchange = getattr(orderbook, 'exchange', 'UNKNOWN')
        symbol = getattr(orderbook, 'symbol', 'UNKNOWN')
        ts = getattr(orderbook, 'timestamp', None)

        bids = getattr(orderbook.book, 'bids', {})
        asks = getattr(orderbook.book, 'asks', {})

        best_bid = max(bids.keys()) if bids else None
        best_ask = min(asks.keys()) if asks else None

        print("[IN] OrderBook event")
        print(f"  exchange={exchange} symbol={symbol} ts={ts} receipt_ts={receipt_timestamp}")
        print(f"  bid_levels={len(bids)} ask_levels={len(asks)}")
        if best_bid is not None and best_ask is not None:
            print(f"  best_bid={best_bid} size={bids[best_bid]} | best_ask={best_ask} size={asks[best_ask]}")

        if symbol != 'BTC':
            print(f"  WARN: inbound symbol is '{symbol}', expected 'BTC'")
    except Exception as exc:
        print(f"[IN] Debug print failed: {exc}")


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

    async def __call__(self, dtype, receipt_timestamp: float):
        if isinstance(dtype, dict):
            data = dtype
        else:
            data = dtype.to_dict(numeric_type=self.numeric_type, none_to=self.none_to)
            if not dtype.timestamp:
                data['timestamp'] = receipt_timestamp
            data['receipt_timestamp'] = receipt_timestamp
        await self.write(data)

    async def __connect(self):
        if not self.producer:
            loop = asyncio.get_event_loop()
            self.producer = AIOKafkaProducer(acks=0,
                                             loop=loop,
                                             bootstrap_servers=f'{self.bootstrap}:{self.port}' if isinstance(self.bootstrap, str) else self.bootstrap,
                                             client_id='cryptofeed')
            await self.producer.start()
            # Ensure topic exists (best-effort)
            await self.__ensure_topic_exists()

    async def __ensure_topic_exists(self):
        if not self.topic:
            return
        # Only attempt if admin client is available
        if AIOKafkaAdminClient is None or NewTopic is None:
            return
        try:
            admin = AIOKafkaAdminClient(bootstrap_servers=f'{self.bootstrap}:{self.port}' if isinstance(self.bootstrap, str) else self.bootstrap)
            await admin.start()
            try:
                new_topics = [NewTopic(name=self.topic, num_partitions=1, replication_factor=1)]
                await admin.create_topics(new_topics=new_topics, validate_only=False)
            except Exception:
                # Topic may already exist or broker may forbid admin ops; ignore
                pass
            finally:
                await admin.close()
        except Exception:
            # Ignore admin failures, producer can still work if topic exists
            pass

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
            # Pre-transform debug
            try:
                sym_pre = data.get('symbol')
                print(f"[OUT.pre] exchange={data.get('exchange')} symbol={sym_pre} raw_ts={data.get('timestamp')} raw_receipt={data.get('receipt_timestamp')}")
                if sym_pre != 'BTC':
                    print(f"[OUT.pre] WARN: outbound (pre) symbol is '{sym_pre}', expected 'BTC'")
            except Exception:
                pass

            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)

            # Safely extract orderbook maps
            book = data.get('book') or {}
            bid_src = {}
            ask_src = {}
            if isinstance(book, dict):
                bid_src = book.get('bid') or {}
                ask_src = book.get('ask') or {}

            # Sort bids desc, asks asc
            data['bid'] = OrderedDict(sorted(bid_src.items(), reverse=True))
            data['ask'] = OrderedDict(sorted(ask_src.items()))

            # Safely remove optional fields
            data.pop('book', None)
            data.pop('delta', None)

            # Post-transform debug
            try:
                best_bid_px = next(iter(data['bid'])) if data.get('bid') else None
                best_ask_px = next(iter(data['ask'])) if data.get('ask') else None
                print(
                    f"[OUT] topic={self.topic} exchange={data.get('exchange')} symbol={data.get('symbol')} "
                    f"ts_ns={data.get('ts')} bid_levels={len(data.get('bid', {}))} ask_levels={len(data.get('ask', {}))}"
                )
                if best_bid_px is not None and best_ask_px is not None:
                    print(f"[OUT] best_bid={best_bid_px} size={data['bid'][best_bid_px]} | best_ask={best_ask_px} size={data['ask'][best_ask_px]}")
                if data.get('symbol') != 'BTC':
                    print(f"[OUT] WARN: outbound symbol is '{data.get('symbol')}', expected 'BTC'")
            except Exception:
                pass

            await self.producer.send_and_wait(self.topic, orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS))  # orjson uses UTF-8 encoding by default
        except:
            print("WARNING: ClickHouseBookKafka.write() didn't fire - go check!")
            pass
