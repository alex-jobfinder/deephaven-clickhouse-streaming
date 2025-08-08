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

# class ClickHousePositionsKafka(KafkaCallback):
#     default_topic = 'hyperliquid_positions'

#     async def write(self, data: dict):
#         await self._KafkaCallback__connect()
#         try:
#             # Map the data to match the expected JSON format
#             formatted_data = {
#                 'user_id': data.get('user', ''),
#                 'timestamp': data.get('timestamp', ''),
#                 'time': int(data.get('ts', 0) / 1_000_000_000) if data.get('ts') else 0,
#                 'coin': data.get('coin', ''),
#                 'szi': str(data.get('szi', 0)),
#                 'leverage_type': data.get('leverage_type', 'cross'),
#                 'leverage_value': float(data.get('leverage', 0)),
#                 'entry_px': str(data.get('entry_px', 0)),
#                 'position_value': str(data.get('position_value', 0)),
#                 'unrealized_pnl': str(data.get('unrealized_pnl', 0)),
#                 'return_on_equity': str(data.get('return_on_equity', 0)),
#                 'liquidation_px': str(data.get('liquidation_px', 0)),
#                 'margin_used': str(data.get('margin_used', 0)),
#                 'max_leverage': float(data.get('max_leverage', 0)),
#                 'funding_all_time': str(data.get('funding_all_time', 0)),
#                 'funding_since_open': str(data.get('funding_since_open', 0)),
#                 'funding_since_change': str(data.get('funding_since_change', 0))
#             }

#             await self.producer.send_and_wait(
#                 self.topic,
#                 orjson.dumps(formatted_data).encode('utf-8')
#             )
#         except Exception as e:
#             print(f"WARNING: ClickHousePositionsKafka.write() failed: {e}")
#             print(f"Data: {data}")


# class ClickHouseFillsKafka(KafkaCallback):
#     default_topic = 'fills'

#     async def write(self, data: dict):
#         await self._KafkaCallback__connect()
#         try:
#             data['ts'] = int(data.pop('time') * 1_000_000_000)
#             data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
#             data['order_id'] = str(data.pop('oid'))
#             data['fill_id'] = data.pop('hash')
#             data['price'] = float(data.pop('px'))
#             data['size'] = float(data.pop('sz'))
#             data['start_position'] = float(data.pop('startPosition', 0))
#             data['pnl'] = float(data.pop('closedPnl', 0))
#             data['crossed'] = bool(data.pop('crossed', False))
#             data['direction'] = data.pop('dir', '')
#             data['side'] = 'buy' if data.pop('side') == 'B' else 'sell'
#             data['coin'] = data.pop('coin')

#             # Optional liquidation data
#             liquidation = data.pop('liquidation', None)
#             if liquidation:
#                 data['liquidated_user'] = liquidation.get('liquidatedUser')
#                 data['mark_price'] = float(liquidation.get('markPx', 0))
#                 data['liquidation_method'] = liquidation.get('method')

#             await self.producer.send_and_wait(
#                 self.topic,
#                 orjson.dumps(data).encode('utf-8')
#             )
#         except Exception as e:
#             print(f"WARNING: ClickHouseFillsKafka.write() failed: {e}")


class ClickHouseBookKafka(KafkaCallback):
    default_topic = 'orderbooks'

    async def write(self, data: dict):
        await self._KafkaCallback__connect()
        try:
            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            data['bid'] = OrderedDict(sorted(data['book'].pop('bid').items(), reverse=True))
            data['ask'] = OrderedDict(sorted(data['book'].pop('ask').items()))
            del data['book']
            del data['delta']
            await self.producer.send_and_wait(self.topic, orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS))  # orjson uses UTF-8 encoding by default
        except:
            print("WARNING: ClickHouseBookKafka.write() didn't fire - go check!")
            pass
