from typing import Dict, List

from loguru import logger
from quixstreams import Application

from src.kraken_api import KrakenWebSocketTradeAPI
from src.config import config


def produce_trade(kafka_broker_address: str, 
                  kafka_topic: str,
                  product_id: str) -> None:
    """
    Produce trade data from the Kraken API to a Kafka topic

    args:
    kafka_broker_address: str : The address of the Kafka broker
    kafka_topic: str : The name of the Kafka topic to produce the trade data to
    product_id: str : The product_id to get the trade data for

    return:
    None
    """
    app = Application(broker_address=kafka_broker_address)

    topic = app.topic(name=kafka_topic, value_serializer='json')

    # Create an instance of the KrakenWebSocketTradeAPI
    kraken_api = KrakenWebSocketTradeAPI(product_id=product_id)

    logger.info('Creating a Producer instance')

    # Create a Producer instance
    with app.get_producer() as producer:
        while True:
            # get the trades from the Kraken API
            trades: List[Dict] = kraken_api.get_trades()
            for trade in trades:
                # Serialize an event using the defined Topic
                message = topic.serialize(key=trade['product_id'], value=trade)

                # Produce a message into the Kafka topic
                producer.produce(topic=topic.name, value=message.value, key=message.key)

                # message = f"{message.key}: {message.value}"
                # print(f"Produced message: {message}")
                logger.info(trades)

                # Sleep for 1 second
                from time import sleep

                sleep(1)


if __name__ == '__main__':
    produce_trade(
        kafka_broker_address= config.kafka_broker_address , 
        kafka_topic=config.kafka_topic,
        product_id=config.product_id
        )
