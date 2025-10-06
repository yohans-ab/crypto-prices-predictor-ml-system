from typing import List, Dict
from websocket import create_connection
import json
from datetime import datetime, timezone
from loguru import logger

# Define a class that will represent the Kraken WebSocket Trade API
class KrakenWebSocketTradeAPI:
    url = 'wss://ws.kraken.com/v2'

    def __init__(self,
                product_id: str,
            
    ):
        
        self.product_id = product_id
    

        # establish connection to the kraken websocket
        self._ws = create_connection(self.url)
        logger.info("Connection established")

        # subscribe to the trade channel for the given product_id
        self._subscribed(product_id=self.product_id)

    # Subscribe to the trade channel for the given product_id
    def _subscribed(self, product_id: str):
        """
        establish connection ot the kraken websocket and subscribe to the trade channel for the given product_id
        """

        logger.info(f"Subscribing to trade channel for product_id: {product_id}")

        #lets subscribe to the trade channel fro the gievn product_id
        msg = {
            "method": "subscribe",
            "params": {
                "channel": "trade",
                "symbol": [
                    product_id,
                ],
                "snapshot": False
            }
        }

        self._ws.send(json.dumps(msg))

        logger.info("Subscribed to trade channel")

        # dump the first two messages we got from the websocket becasue they are confirmation messages
        _ = self._ws.recv()
        _ = self._ws.recv()

        # return _subscribed
        # self._subscribed(product_id=self.product_id)

    
    # Get the trades from the Kraken WebSocket Trade API
    
    def get_trades(self) -> List[Dict]:
        

        # mock_trades = [
        #     {
        #         "product_id": "BTC/USD",
        #         "price": 62000,
        #         "volume": 0.01,
        #         "timestamp": 1640000000   
        #     },
        #     {
        #         "product_id": "BTC/USD",
        #         "price": 58000,
        #         "volume": 0.01,
        #         "timestamp": 1660000000   
        #     }
        # ]
        
        message =self._ws.recv()
        if "heartbeat" in message:
            #if we receive a heartbeat message, we will return an empty list
            return []
        message = json.loads(message)
        # logger.info(f"Received message: {message}")
        
        #parse the message and extract the trade data
      

        trades =[]
        for trade in message['data']:
            trades.append({
                    "product_id": self.product_id,
                    "price": float(trade['price']),
                    "volume": float(trade['qty']),
                    "timestamp": trade['timestamp'], 
                })
            
            
        return trades
            

