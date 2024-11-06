import pika
import json
import sys

def callback(ch, method, properties, body):
    try:
        # Decode the JSON message
        message = json.loads(body)
        print(f" [x] Received message: {message}")

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError as e:
        print("Failed to decode JSON message:", e)
        # Optionally, you can negatively acknowledge the message
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    try:
        # Establish connection to RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672))
        channel = connection.channel()

        # Declare the queue to consume from
        channel.queue_declare(queue='test_queue', durable=True)

        # Set QoS to prefetch one message at a time
        channel.basic_qos(prefetch_count=1)

        # Set up subscription with the callback function
        channel.basic_consume(queue='test_queue', on_message_callback=callback, auto_ack=False)

        print(' [*] Waiting for messages. To exit press CTRL+C')

        # Start consuming
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        print("Failed to connect to RabbitMQ server:", e)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        try:
            if connection and connection.is_open:
                connection.close()
        except:
            pass
        sys.exit(0)

if __name__ == "__main__":
    main()
