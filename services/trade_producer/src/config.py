import os
from dotenv import load_dotenv, find_dotenv

#load my environment variables
load_dotenv(find_dotenv())


#add pydantic settings
from pydantic import BaseSettings

class Config(BaseSettings):
    product_id: str = os.environ['PRODUCT_ID']
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    kafka_topic_name: str = 'trade'

    # ohlc_window_seconds : int = os.environ['OHLC_WINDOW_SECONDS']

config = Config()

# producer_id='ETH/EUR'
# kafka_broker_address=os.getenv('KAFKA_BROKER_ADDRESS')
# kafka_topic='trade'


