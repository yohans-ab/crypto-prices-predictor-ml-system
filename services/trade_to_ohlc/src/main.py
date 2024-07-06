
from loguru import logger
import json

def trade_to_ohlc(
        kafka_input_topic: str,
        kafak_output_topic: str,
        kafka_broker_address: str,
        ohlc_window_seconds: int
    )-> None:

    """
    Consume trade data from a Kafka topic, calculate the OHLC data and produce it to another Kafka topic
    
    args:
    kafka_input_topic: str : The name of the Kafka topic to consume the trade data from
    kafka_output_topic: str : The name of the Kafka topic to produce the OHLC data to
    kafka_broker_address: str : The address of the Kafka broker
    ohlc_window_seconds: int : The time window in seconds to calculate the OHLC data for
    
    return:
    None
    """

    # Create a Kafka consumer
    from quixstreams import Application

    # this handles low level kafka consumer operations

    app = Application(broker_address=kafka_broker_address,
                      consumer_group='trade_to_ohlc',
                      auto_offset_reset='earliest'
                      )
    # specify the input and output topics
    input_topic = app.topic(name=kafka_input_topic, value_deserializer='json')
    output_topic = app.topic(name=kafak_output_topic, value_serializer='json')

    #create a quickstreams dataframe
    sdf = app.dataframe(input_topic)

    #Add transforamtion jobs to the dataframe fro the incoming data to produce candles
    # create tumbling window
    from datetime import timedelta
    
    # calculate the OHLC data
    def init_ohlc_candle(value:dict) -> dict:
        """
        Initialize the OHLC candle with the first trade data
        """
        return {
            'open': value['price'],
            'high': value['price'],
            'low': value['price'],
            'close': value['price'],
            'product_id': value['product_id'],
        }
    def update_ohlc_candle(ohlc_cadle:dict, trade:dict) -> dict:
        """
        Update the OHLC candle with the new trade data

        args:
        ohlc_cadle: dict : The current OHLC candle
        trade: dict : The new trade data

        return:
        dict : The updated OHLC candle
        """
        return {
            'open': ohlc_cadle['open'],
            'high': max(ohlc_cadle['high'], trade['price']),
            'low': min(ohlc_cadle['low'], trade['price']),
            'close': trade['price'],
            'product_id': trade['product_id'],
        }
        # trade['high'] = max(ohlc_cadle['high'], trade['price'])
        # trade['low'] = min(ohlc_cadle['low'], trade['price'])
        # trade['close'] = trade['price']
        # trade['product_id'] = trade['product_id']
        # return trade    
    
    # create a tumbling window of the specified duration and calculate the OHLC data
    sdf = sdf.tumbling_window(duration_ms = timedelta(seconds=ohlc_window_seconds))
    sdf = sdf.reduce(reducer= update_ohlc_candle, initializer=init_ohlc_candle).final()

    # extract the open, high, low, close, timestamp (the end) and product_id from the sdf and log it
    sdf['open'] = sdf['value']['open']
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf['close'] = sdf['value']['close']
    sdf['product_id'] = sdf['value']['product_id']
    sdf['timestamp'] = sdf['end']
    
    # keep only the necessary columns/keys in the dataframe
    sdf = sdf[['timestamp', 'open', 'high', 'low', 'close', 'product_id']]

    # log the OHLC data
    sdf= sdf.update(logger.info)

    # write output topic to the dataframe
    sdf = sdf.to_topic(output_topic)
    
    #kick off the application
    app.run(sdf)

if __name__ == '__main__':

    # read environment variables
    from src.config import config
    
    trade_to_ohlc(
        kafka_input_topic=config.kafka_input_topic,
        kafak_output_topic=config.kafka_output_topic,
        kafka_broker_address=config.kafka_broker_address,
        ohlc_window_seconds=config.ohlc_window_seconds
    )


