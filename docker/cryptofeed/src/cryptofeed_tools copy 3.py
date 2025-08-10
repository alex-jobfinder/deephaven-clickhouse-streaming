import asyncio
import logging
import sys
from collections import OrderedDict, defaultdict
from typing import Optional, ByteString
from datetime import datetime

import orjson
from aiokafka import AIOKafkaProducer
from aiokafka.errors import RequestTimedOutError, KafkaConnectionError, NodeNotReadyError

# Enhanced logging setup
def setup_logging(service_name: str, log_level: str = "INFO"):
    """Setup comprehensive logging for the service"""
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create handlers
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # File handler for errors
    error_handler = logging.FileHandler(f'/cryptofeed/logs/{service_name}_errors.log')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    # File handler for all logs
    file_handler = logging.FileHandler(f'/cryptofeed/logs/{service_name}_all.log')
    file_handler.setFormatter(formatter)
    
    # Setup logger
    logger = logging.getLogger(service_name)
    logger.setLevel(getattr(logging, log_level.upper()))
    logger.addHandler(console_handler)
    logger.addHandler(error_handler)
    logger.addHandler(file_handler)
    
    return logger

# Initialize logger
logger = setup_logging('cryptofeed_tools')

# Import the backend classes from cryptofeed
try:
    from cryptofeed.backends.backend import BackendBookCallback, BackendCallback, BackendQueue
except ImportError:
    # Fallback if cryptofeed backend classes aren't available
    class BackendQueue:
        def __init__(self):
            pass
    
    class BackendCallback:
        def __init__(self):
            pass
    
    class BackendBookCallback:
        def __init__(self):
            pass

LOG = logging.getLogger('feedhandler')

SYMBOLS = ['BTC-USD']
SYMBOLS_HYPERLIQUID = ["BTC"]

async def my_print(data, _receipt_time):
    print(data)

class KafkaCallback(BackendQueue):
    def __init__(self, key=None, numeric_type=float, none_to=None, **kwargs):
        """
        You can pass configuration options to AIOKafkaProducer as keyword arguments.
        (either individual kwargs, an unpacked dictionary `**config_dict`, or both)
        A full list of configuration parameters can be found at
        https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaProducer

        A 'value_serializer' option allows use of other schemas such as Avro, Protobuf etc.
        The default serialization is JSON Bytes

        Example:

            **{'bootstrap_servers': '127.0.0.1:9092',
            'client_id': 'cryptofeed',
            'acks': 1,
            'value_serializer': your_serialization_function}

        (Passing the event loop is already handled)
        """
        self.producer_config = kwargs
        self.producer = None
        self.key: str = key or self.default_key
        self.numeric_type = numeric_type
        self.none_to = none_to
        # Do not allow writer to send messages until connection confirmed
        self.running = False
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        
        # Log initialization
        self.logger.info(f"Initializing {self.__class__.__name__} with key: {self.key}")
        self.logger.debug(f"Producer config: {self.producer_config}")

    def _default_serializer(self, to_bytes: dict | str) -> ByteString:
        if isinstance(to_bytes, dict):
            return orjson.dumps(to_bytes)
        elif isinstance(to_bytes, str):
            return to_bytes.encode()
        else:
            raise TypeError(f'{type(to_bytes)} is not a valid Serialization type')

    async def _connect(self):
        if not self.producer:
            loop = asyncio.get_event_loop()
            try:
                config_keys = ', '.join([k for k in self.producer_config.keys()])
                self.logger.info(f'Configuring AIOKafka with parameters: {config_keys}')
                self.producer = AIOKafkaProducer(**self.producer_config, loop=loop)
            # Quit if invalid config option passed to AIOKafka
            except (TypeError, ValueError) as e:
                self.logger.error(f'Invalid AIOKafka configuration: {e.args}')
                raise SystemExit
            else:
                while not self.running:
                    try:
                        await self.producer.start()
                    except KafkaConnectionError:
                        self.logger.error(f'Unable to bootstrap from host(s) - retrying in 10s')
                        await asyncio.sleep(10)
                    else:
                        self.logger.info(f'Connected to cluster with {len(self.producer.client.cluster.brokers())} broker(s)')
                        self.running = True

    def topic(self, data: dict) -> str:
        return f"{self.key}-{data['exchange']}-{data['symbol']}"

    def partition_key(self, data: dict) -> Optional[bytes]:
        return None

    def partition(self, data: dict) -> Optional[int]:
        return None

    async def writer(self):
        await self._connect()
        while self.running:
            async with self.read_queue() as updates:
                for index in range(len(updates)):
                    topic = self.topic(updates[index])
                    # Check for user-provided serializers, otherwise use default
                    value = updates[index] if self.producer_config.get('value_serializer') else self._default_serializer(updates[index])
                    key = self.key if self.producer_config.get('key_serializer') else self._default_serializer(self.key)
                    partition = self.partition(updates[index])
                    
                    try:
                        send_future = await self.producer.send(topic, value, key, partition)
                        await send_future
                        self.logger.debug(f'Message sent to topic: {topic}')
                    except RequestTimedOutError:
                        self.logger.error(f'No response received from server within {self.producer._request_timeout_ms} ms')
                    except NodeNotReadyError:
                        self.logger.error(f'Node not ready')
                    except Exception as e:
                        self.logger.error(f'Encountered an error: {e}', exc_info=True)
        
        self.logger.info(f"Sending last messages and closing connection")
        await self.producer.stop()

class ClickHouseTradeKafka(KafkaCallback, BackendCallback):
    default_key = 'trades'
    default_topic = 'trades'

    async def write(self, data: dict):
        try:
            await self._connect()
            
            # Log incoming data
            self.logger.debug(f"Processing trade data: {data.get('symbol', 'unknown')} from {data.get('exchange', 'unknown')}")
            
            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            data['size'] = data.pop('amount')
            data['trade_id'] = data.pop('id')
            del data['type']
            
            # Create key for Kafka message
            if 'exchange' in data and 'symbol' in data:
                kafka_key = f"{data['exchange']}_{data['symbol']}".encode('utf-8')
            else:
                kafka_key = None
                self.logger.warning(f"Missing exchange or symbol for Kafka key: {data.get('exchange', 'unknown')}, {data.get('symbol', 'unknown')}")
            
            # Send with key
            await self.producer.send_and_wait(self.default_topic, orjson.dumps(data), key=kafka_key)
            self.logger.debug(f"Trade sent to Kafka: {data.get('symbol')} from {data.get('exchange')}")
            
        except Exception as e:
            self.logger.error(f"ClickHouseTradeKafka.write() failed: {e}", exc_info=True)
            self.logger.error(f"Data keys: {list(data.keys()) if data else 'None'}")
            # Re-raise to ensure the error is not silently ignored
            raise

class ClickHouseBookKafka(KafkaCallback, BackendBookCallback):
    default_key = 'book'
    default_topic = 'orderbooks'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)
        self.logger.info(f"Initialized BookKafka: snapshots_only={snapshots_only}, interval={snapshot_interval}")

    async def write(self, data: dict):
        try:
            await self._connect()
            
            # Log incoming data
            self.logger.debug(f"Processing orderbook data: {data.get('symbol', 'unknown')} from {data.get('exchange', 'unknown')}")
            
            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            data['bid'] = OrderedDict(sorted(data['book'].pop('bid').items(), reverse=True))
            data['ask'] = OrderedDict(sorted(data['book'].pop('ask').items()))
            del data['book']
            del data['delta']
            
            # Create key for Kafka message
            if 'exchange' in data and 'symbol' in data:
                kafka_key = f"{data['exchange']}_{data['symbol']}".encode('utf-8')
            else:
                kafka_key = None
                self.logger.warning(f"Missing exchange or symbol for Kafka key: {data.get('exchange', 'unknown')}, {data.get('symbol', 'unknown')}")
            
            # Ensure exchange field is preserved
            if 'exchange' not in data:
                self.logger.warning(f"Missing exchange field in orderbook data: {data.get('symbol', 'unknown')}")
            
            # Send with key
            await self.producer.send_and_wait(self.default_topic, orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS), key=kafka_key)
            self.logger.debug(f"Orderbook sent to Kafka: {data.get('symbol')} from {data.get('exchange')} - Bids: {len(data['bid'])}, Asks: {len(data['ask'])}")
            
        except Exception as e:
            self.logger.error(f"ClickHouseBookKafka.write() failed: {e}", exc_info=True)
            self.logger.error(f"Data keys: {list(data.keys()) if data else 'None'}")
            raise

# Additional Kafka callback classes for future use
class TradeKafka(KafkaCallback, BackendCallback):
    default_key = 'trades'

class FundingKafka(KafkaCallback, BackendCallback):
    default_key = 'funding'

class BookKafka(KafkaCallback, BackendBookCallback):
    default_key = 'book'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)

class TickerKafka(KafkaCallback, BackendCallback):
    default_key = 'ticker'

class OpenInterestKafka(KafkaCallback, BackendCallback):
    default_key = 'open_interest'

class LiquidationsKafka(KafkaCallback, BackendCallback):
    default_key = 'liquidations'

class CandlesKafka(KafkaCallback, BackendCallback):
    default_key = 'candles'

class OrderInfoKafka(KafkaCallback, BackendCallback):
    default_key = 'order_info'

class TransactionsKafka(KafkaCallback, BackendCallback):
    default_key = 'transactions'

class BalancesKafka(KafkaCallback, BackendCallback):
    default_key = 'balances'

class FillsKafka(KafkaCallback, BackendCallback):
    default_key = 'fills'