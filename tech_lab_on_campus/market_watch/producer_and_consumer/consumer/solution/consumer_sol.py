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
    
    def setupRMQConnection(self):

        # Set-up Connection to RabbitMQ service
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=conParams)

        # Establish Channel
        self.channel = self.connection.channel()

        # Create Queue if not already present
        self.channel.queue_declare(self.queue_name)

        # Create the exchange if not already present
        channel_exchange = self.channel.exchange_declare(exchange=self.exchange_name)

        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(exchange=channel_exchange, queue=self.queue_name, routing_key=self.binding_key)

        # Set-up Callback function for receiving messages
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_message_callback)
    
    def on_message_callback(self, channel, method_frame, header_frame, body):
        # Acknowledge message
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        #Print message (The message is contained in the body parameter variable)
        print(f" [x] Received {body.decode()}")
    
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
        self.channel.close()

        # Close Connection
        self.connection.close()

        

        
    