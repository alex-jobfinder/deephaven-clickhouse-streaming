from cryptofeed import FeedHandler
from cryptofeed.defines import L2_BOOK
from cryptofeed.exchanges import Coinbase, Kraken, Bitstamp, HyperLiquid

import src.cryptofeed_tools as cft
import os


def main():
    """
    Example runner for L2 orderbook streaming.

    Data path (HyperLiquid):
      1) This script registers a subscription for L2_BOOK with `SYMBOLS_HYPERLIQUID`.
      2) The `HyperLiquid` feed subscribes on the websocket with { type: 'l2Book', coin }.
      3) The exchange sends WsBook snapshots (coin, levels, time).
      4) The feed normalizes snapshots into a cryptofeed OrderBook using book[BID]/book[ASK].
      5) `callbacks[L2_BOOK]` receive the OrderBook; here `ClickHouseBookKafka` flattens
         the structure and writes into the 'orderbooks' Kafka topic, while `my_print`
         logs the events.
    """
    # see docker_files/Dockerfile.cryptofeed where we set IS_DOCKER=True
    # by doing this here, we can also run this script locally
    # see https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/#scenario-4
    kakfa_bootstrap = 'redpanda' if os.environ.get('IS_DOCKER') else 'localhost'
    kakfa_port = 29092 if os.environ.get('IS_DOCKER') else 9092

    # ch_book_kafka = cft.ClickHouseBookKafka(bootstrap=kakfa_bootstrap, port=kakfa_port,
    #                                         snapshot_interval=25000, snapshots_only=True)
    ch_book_kafka = cft.ClickHouseBookKafka(bootstrap=kakfa_bootstrap, port=kakfa_port)

    callbacks = {L2_BOOK: [ch_book_kafka, cft.my_print]}

    # cft.SYMBOLS = ['BTC-USD']   # for testing

    f = FeedHandler()
    f.add_feed(Coinbase(max_depth=2000, channels=[L2_BOOK], symbols=cft.SYMBOLS, callbacks=callbacks))
    f.add_feed(Bitstamp(channels=[L2_BOOK], symbols=cft.SYMBOLS, callbacks=callbacks))
    f.add_feed(Kraken(channels=[L2_BOOK], symbols=cft.SYMBOLS, callbacks=callbacks))  
    f.add_feed(HyperLiquid(
        subscription={
            L2_BOOK: cft.SYMBOLS_HYPERLIQUID
        },
        callbacks={
            L2_BOOK: [ch_book_kafka, cft.my_print]
        }))          
    f.run()


if __name__ == '__main__':
    main()
