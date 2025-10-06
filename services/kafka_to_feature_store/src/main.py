from quixstreams import Application
import json
from loguru import logger
from src.hopswork_api import push_data_to_feature_store

def kafka_to_feature_store(
        kafka_topic: str,
        kafka_broker_address: str,
        feature_group_name: str,
        feature_group_version: int,
    )-> None:
    """
    This function reads ohlc data from a kafka topic and writes it to a feature store.
    
    args:
    kafka_topic: str: The kafka topic to read data from.
    kafka_broker_address: str: The address of the kafka broker.
    feature_group_name: str: The name of the feature group to write to.
    feature_group_version: int: The version of the feature group to write to.
    
    returns:
    
    None
    """

    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="kafka_to_feature_store",
    )

    input_topic = app.topic(name=kafka_topic, value_serializer="json")


    with app.get_consumer() as consumer:
        consumer.subscribe([kafka_topic])

        while True:
            msg = consumer.poll(timeout=1.0)

            
            if msg is None:
                continue

            elif msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            else :
                # parse the message
                ohlc = json.loads(msg.value().decode('utf-8'))
                
                # Write the data to the feature store
                # feature_store.write()
                # create a fucntion to push data to feature store function
                push_data_to_feature_store(
                    feature_group_name=feature_group_name,
                    feature_group_version=feature_group_version,
                    data=ohlc
                )
            
            consumer.store_offsets(msg)



            # breakpoint()
            
            

if __name__ == "__main__":
    kafka_to_feature_store(
        kafka_topic="ohlc",
        kafka_broker_address="localhost:19092",
        feature_group_name="ohlc_feature_group",
        feature_group_version=1,
    )


