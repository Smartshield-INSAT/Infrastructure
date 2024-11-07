import subprocess
import threading
import time
import json
import requests
import pika  # Import pika for RabbitMQ
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import base64
import pandas as pd
from io import BytesIO
import httpx
import os
import asyncio
from feature_processing import process_file

# Configuration
RABBITMQ_URL = "amqp://guest:guest@localhost:5672"  # Default port for RabbitMQ is 5672
QUEUE_NAME = "testQueue"
API_CIC = "http://api-iyed-endpoint"
API_NB15 = "http://192.168.100.88:8002/predict-all"


def run_preprocessing(script_name, data):
    """Runs a preprocessing script with the provided data."""
    res = subprocess.run([f"./scripts/{script_name}", data], check=True)  # Run the script
    return res


async def process_message(data):
    """Processes a single message by sending a file to the API."""
    with open("data.pcap", "ab") as dt:
        dt.write(data)
    process_file("10_samples.parquet")
    with open("10_samples.parquet", "rb") as file:
        parquet_buffer = BytesIO(file.read())
    
    async with httpx.AsyncClient() as client:
        files = {'file': ("10_samples.parquet", parquet_buffer, "application/octet-stream")}
        response = await client.post(API_NB15, files=files)

    return response.text, ""  # Placeholder for res_cic


async def consume_messages():
    """Consumes messages from RabbitMQ asynchronously and processes them."""
    # Establish a connection using pika
    print('Waiting for messages. To exit press CTRL+C')

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # Declare the queue (ensure it exists)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    
    # Consume messages from the queue
    async def callback(ch, method, properties, body):
        data = body
        res_nb15, res_cic = await process_message(data)
        print(res_nb15, '\n', res_cic)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Start consuming messages
    for method_frame, properties, body in channel.consume(queue=QUEUE_NAME, auto_ack=False):
        await callback(channel, method_frame, properties, body)



def main():
    # Start RabbitMQ in a separate thread if needed
    # rabbitmq_thread = threading.Thread(target=start_rabbitmq, daemon=True)
    # rabbitmq_thread.start()

    # Start consuming messages
    asyncio.run(consume_messages())


if __name__ == "__main__":
    main()
