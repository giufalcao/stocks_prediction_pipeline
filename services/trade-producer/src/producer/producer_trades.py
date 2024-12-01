from quixstreams import Application
from src.websocket.kraken_api import KrakenWebsocketTradeAPI


def producer_trades(kafka_broker_address: str, kafka_topic_name: str) -> None:
    """
    Read trades rom the Kraken websocket API ans saves them into a Kafka topic.

    Args:
        - kafka_broker_address (str): The addess of the kafka broker
        - kafka_topic_name (str): The name of the kafka topic
    
    Returns:
        None
    """

    app = Application(broker_address=kafka_broker_address)
    topic = app.topic(name=kafka_topic_name, value_serializer="json")

    kraken_api = KrakenWebsocketTradeAPI()

    # Create a Producer instance
    with app.get_producer() as producer:

        # Serialize an event using the defined Topic 
        trades = kraken_api.get_trades()

        for trade in trades:
            message = topic.serialize(key=trade["product_id"], value=trade)

            # Produce a message into the Kafka topic
            print("message sent")
            producer.produce(topic=topic.name, value=message.value, key=message.key)

if __name__ == "__main__":
    producer_trades(kafka_broker_address="localhost:19092", kafka_topic_name="trade")
