from . import mqProducerInterface
import pika
import os


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
        connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        channel = connection.channel()

        # Create the exchange if not already present
        exchange = channel.exchange_declare(exchange="Exchange Name")


    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        self.channel.basic_publish(
            exchange="Exchange Name",
            routing_key="Routing Key",
            body="Message",
        )

        # Close Channel
        self.channel.close()

        # Close Connection
        self.connection.close()