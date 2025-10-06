import os
from dotenv import load_dotenv, find_dotenv

#load my environment variables
load_dotenv(find_dotenv())


#add pydantic settings
from pydantic_settings import BaseSettings

class Config(BaseSettings):
    product_id: str = 'ETH/USD'
    kafka_broker_address: str = 'localhost:19092'
    kafka_input_topic: str = 'trade'
    kafka_output_topic: str = 'ohlc'
    ohlc_window_seconds : int = 10

config = Config()