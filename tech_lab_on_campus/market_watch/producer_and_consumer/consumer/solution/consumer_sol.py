import sys
import os
import pika

from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name

        self.setupRMQConnection()

    
    def setupRMQConnection(self)

        # Set-up Connection to RabbitMQ service
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        connection = pika.BlockingConnection(pika.ConnectionParameters(paramaters=conParams))
        # Establish Channel
        self.channel = connection.channel()

        # Create Queue if not already present
        self.hannel.queue_declare(self.queue_name)

        # Create the exchange if not already present
        channel_exchange = self.channel.exhange_declare(exchange=self.exchange_name, exchange_type='direct', durable=True)

        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(exchange=channel_exchange, queue=self.queue_name, routing_key=self.binding_key)

        # Set-up Callback function for receiving messages
        self.channel.start_consuming()
    
    def on_message_callback(self):
        # Acknowledge message
        self.channel.basic_ack(delivery_tag=self.method.delivery_tag)

        #Print message (The message is contained in the body parameter variable)
        print(f" [x] Received {self.body.decode()}")
    
    def startConsuming(self):
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(' [*] Waiting for messages. To exit press CTRL+C')
        
        # Start consuming messages
        self.channel.start_consuming()

    def __del__(self):
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        # Close Channel
        self.channel.stop_consuming()

        # Close Connection
        self.channel.close()

        

        
    