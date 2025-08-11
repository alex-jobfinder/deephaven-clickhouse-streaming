import asyncio
from collections import OrderedDict

import orjson
from aiokafka import AIOKafkaProducer

SYMBOLS = ['BTC-USD', 'ETH-USD', 'AVAX-USD', 'SOL-USD']
SYMBOLS_HYPERLIQUID = ["ADA", "APT", "ATOM", "AVAX", "BNB", "BTC", "DOGE", "DOT", "ETH", "FARTCOIN", "HYPE", "NEAR", "SOL", "SUI", "TIA", "XRP"]

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
        await self.__connect()  # Fix: use parent's method name
        try:
            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            data['size'] = data.pop('amount')
            data['trade_id'] = data.pop('id')
            del data['type']
            
            # ✅ Create key for Kafka message (exchange_symbol) - REQUIRED for ClickHouse
            if 'exchange' in data and 'symbol' in data:
                kafka_key = f"{data['exchange']}_{data['symbol']}".encode('utf-8')
            else:
                kafka_key = None
                print(f"WARNING: Missing exchange or symbol for Kafka key: {data.get('exchange', 'unknown')}, {data.get('symbol', 'unknown')}")
            
            # ✅ Send with key
            await self.producer.send_and_wait(self.topic, orjson.dumps(data), key=kafka_key)
        except Exception as e:  # Fix: use proper exception handling
            print(f"WARNING: ClickHouseTradeKafka.write() failed: {e}")
            print(f"Data keys: {list(data.keys()) if data else 'None'}")
            pass



class ClickHouseBookKafka(KafkaCallback):
    default_topic = 'orderbooks'

    async def write(self, data: dict):
        await self.__connect()  # Fix: use parent's method name
        try:
            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            data['bid'] = OrderedDict(sorted(data['book'].pop('bid').items(), reverse=True))
            data['ask'] = OrderedDict(sorted(data['book'].pop('ask').items()))
            del data['book']
            del data['delta']
            
            # ✅ Create key for Kafka message (exchange_symbol) - REQUIRED for ClickHouse
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
            print(f"Data keys: {list(data.keys()) if data else 'None'}")
            pass