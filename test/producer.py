import pika
import json
import time
import sys

def main():
    try:
        # Establish connection to RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672))
        channel = connection.channel()

        # Declare a queue named 'test_queue'
        channel.queue_declare(queue='test_queue', durable=True)

        # Create a message with a timestamp
        message = {
            "field": "value",
            "timestamp": time.time()
        }
        message_json = json.dumps(message)

        # Publish the message to the queue
        channel.basic_publish(
            exchange='',
            routing_key='test_queue',
            body=message_json,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )
        print(f" [x] Sent message: {message}")

        # Close the connection
        connection.close()

    except pika.exceptions.AMQPConnectionError as e:
        print("Failed to connect to RabbitMQ server:", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
