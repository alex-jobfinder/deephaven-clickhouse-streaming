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
            print("=" * 100)
            print("🚀 CLICKHOUSE KAFKA TRANSFORMATION STARTING")
            print("=" * 100)
            
            # Show original data structure
            print("📥 ORIGINAL ORDERBOOK DATA RECEIVED:")
            print(f"   Exchange: {data.get('exchange', 'N/A')}")
            print(f"   Symbol: {data.get('symbol', 'N/A')}")
            print(f"   Timestamp: {data.get('timestamp', 'N/A')}")
            print(f"   Receipt Timestamp: {data.get('receipt_timestamp', 'N/A')}")
            
            # Show book structure before transformation
            book_data = data.get('book', {})
            bid_count = len(book_data.get('bid', {}))
            ask_count = len(book_data.get('ask', {}))
            print(f"   Book structure - Bids: {bid_count}, Asks: {ask_count}")
            
            # Transform timestamps
            original_ts = data.pop('timestamp')
            original_receipt_ts = data.pop('receipt_timestamp')
            data['ts'] = int(original_ts * 1_000_000_000)
            data['receipt_ts'] = int(original_receipt_ts * 1_000_000_000)
            
            print("🔄 TIMESTAMP TRANSFORMATION:")
            print(f"   Original timestamp: {original_ts}")
            print(f"   Nanosecond timestamp: {data['ts']}")
            print(f"   Original receipt_ts: {original_receipt_ts}")
            print(f"   Nanosecond receipt_ts: {data['receipt_ts']}")
            
            # Transform orderbook data
            book = data['book']
            bid_dict = book.pop('bid')
            ask_dict = book.pop('ask')
            
            # Sort bids (highest price first) and asks (lowest price first)
            sorted_bids = OrderedDict(sorted(bid_dict.items(), reverse=True))
            sorted_asks = OrderedDict(sorted(ask_dict.items()))
            
            data['bid'] = sorted_bids
            data['ask'] = sorted_asks
            
            print("📊 ORDERBOOK SORTING:")
            print(f"   Bids sorted (highest first): {len(sorted_bids)} levels")
            if sorted_bids:
                best_bid_price = next(iter(sorted_bids))
                best_bid_size = sorted_bids[best_bid_price]
                print(f"   Best bid: {best_bid_price} @ {best_bid_size}")
                
            print(f"   Asks sorted (lowest first): {len(sorted_asks)} levels")
            if sorted_asks:
                best_ask_price = next(iter(sorted_asks))
                best_ask_size = sorted_asks[best_ask_price]
                print(f"   Best ask: {best_ask_price} @ {best_ask_size}")
            
            # Clean up unused fields
            del data['book']
            del data['delta']
            
            print("🧹 CLEANUP:")
            print("   Removed 'book' and 'delta' fields")
            
            # Show final data structure
            print("📤 FINAL KAFKA MESSAGE STRUCTURE:")
            print(f"   Keys: {list(data.keys())}")
            print(f"   Exchange: {data.get('exchange')}")
            print(f"   Symbol: {data.get('symbol')}")
            print(f"   Timestamp (ns): {data.get('ts')}")
            print(f"   Bid levels: {len(data.get('bid', {}))}")
            print(f"   Ask levels: {len(data.get('ask', {}))}")
            
            # Serialize and send
            json_data = orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS)
            print(f"📨 KAFKA PUBLISH:")
            print(f"   Topic: {self.topic}")
            print(f"   Message size: {len(json_data)} bytes")
            
            await self.producer.send_and_wait(self.topic, json_data)
            print("✅ Successfully published to Kafka!")
            print("=" * 100)
            
        except Exception as e:
            print(f"❌ ERROR in ClickHouseBookKafka.write(): {e}")
            print("WARNING: ClickHouseBookKafka.write() didn't fire - go check!")
            import traceback
            traceback.print_exc()
