import asyncio
from collections import OrderedDict

import orjson
from aiokafka import AIOKafkaProducer

SYMBOLS = ['BTC-USD']
SYMBOLS_HYPERLIQUID = ["BTC"]

async def my_print(data, _receipt_time):
    print(data)

class KafkaCallback:
    """Base class for Kafka callbacks"""
    
    def __init__(self, bootstrap='localhost', port=9092, topic=None):
        self.bootstrap = bootstrap
        self.port = port
        self.topic = topic or self.default_topic
        self.producer = None
    
    async def _KafkaCallback__connect(self):
        """Connect to Kafka if not already connected"""
        if self.producer is None:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=f"{self.bootstrap}:{self.port}",
                value_serializer=lambda v: v
            )
            await self.producer.start()

class ClickHouseTradeKafka(KafkaCallback):
    default_topic = 'trades'

    async def __call__(self, data, receipt_time):
        """Make the object callable for cryptofeed"""
        await self.write(data)

    async def write(self, data):
        await self._KafkaCallback__connect()
        try:
            # Convert cryptofeed Trade object to dict if needed
            if hasattr(data, 'to_dict'):
                data = data.to_dict()
            elif hasattr(data, '__dict__'):
                data = data.__dict__.copy()
            else:
                data = dict(data)
            
            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            data['size'] = data.pop('amount')
            data['trade_id'] = data.pop('id')
            del data['type']
            
            # ✅ Create key for Kafka message (exchange_symbol)
            if 'exchange' in data and 'symbol' in data:
                kafka_key = f"{data['exchange']}_{data['symbol']}".encode('utf-8')
            else:
                kafka_key = None
                print(f"WARNING: Missing exchange or symbol for Kafka key: {data.get('exchange', 'unknown')}, {data.get('symbol', 'unknown')}")
            
            # ✅ Send with key
            await self.producer.send_and_wait(self.topic, orjson.dumps(data), key=kafka_key)
        except Exception as e:
            print(f"WARNING: ClickHouseTradeKafka.write() failed: {e}")
            print(f"Data type: {type(data)}")
            if hasattr(data, 'to_dict'):
                print(f"Data keys: {list(data.to_dict().keys())}")
            elif hasattr(data, '__dict__'):
                print(f"Data keys: {list(data.__dict__.keys())}")
            else:
                print(f"Data: {data}")
            pass

class ClickHouseBookKafka(KafkaCallback):
    default_topic = 'orderbooks'

    async def __call__(self, data, receipt_time):
        """Make the object callable for cryptofeed"""
        await self.write(data)

    async def write(self, data):
        await self._KafkaCallback__connect()
        try:
            # Convert cryptofeed OrderBook object to dict if needed
            if hasattr(data, 'to_dict'):
                data = data.to_dict()
            elif hasattr(data, '__dict__'):
                data = data.__dict__.copy()
            else:
                data = dict(data)
            
            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            data['bid'] = OrderedDict(sorted(data['book'].pop('bid').items(), reverse=True))
            data['ask'] = OrderedDict(sorted(data['book'].pop('ask').items()))
            del data['book']
            del data['delta']
            
            # ✅ Create key for Kafka message (exchange_symbol)
            if 'exchange' in data and 'symbol' in data:
                kafka_key = f"{data['exchange']}_{data['symbol']}".encode('utf-8')
            else:
                kafka_key = None
                print(f"WARNING: Missing exchange or symbol for Kafka key: {data.get('exchange', 'unknown')}, {data.get('symbol', 'unknown')}")
            
            # ✅ Ensure exchange field is preserved
            if 'exchange' not in data:
                print(f"WARNING: Missing exchange field in orderbook data: {data.get('symbol', 'unknown')}")
            
            # ✅ Send with key
            await self.producer.send_and_wait(self.topic, orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS), key=kafka_key)
        except Exception as e:
            print(f"WARNING: ClickHouseBookKafka.write() failed: {e}")
            print(f"Data type: {type(data)}")
            if hasattr(data, 'to_dict'):
                print(f"Data keys: {list(data.to_dict().keys())}")
            elif hasattr(data, '__dict__'):
                print(f"Data keys: {list(data.__dict__.keys())}")
            else:
                print(f"Data: {data}")
            pass
