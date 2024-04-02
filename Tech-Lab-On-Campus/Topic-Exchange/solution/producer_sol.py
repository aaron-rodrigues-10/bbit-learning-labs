from producer_interface import mqProducerInterface
import pika
import os
import sys
import json


# Constructor: Call the setupRMQConnection function.
# setupRMQConnection Function: Establish connection to the RabbitMQ service.
# publishOrder:  Publish a simple UTF-8 string message from the parameter.
# publishOrder:  Close Channel and Connection.  

class mqProducer(mqProducerInterface):
    # Constructor: Call the setupRMQConnection function.
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        # Save parameters to class variables
        self.routing_key = routing_key
        self.exchange_name = exchange_name

        # Call setupRMQConnection
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = self.connection.channel()

        # Create the topic exchange if not already present
        self.exchange = self.channel.exchange_declare(
            exchange = self.exchange, exchange_type="topic"
        )

    def publishOrder(self, message: str) -> None:
        # Create Appropiate Topic String
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message,
        )

        # Print Confirmation
        print(f"Confirmation Message:" + message)

        # Close channel and connection
        self.channel.close()
        self.connection.close()