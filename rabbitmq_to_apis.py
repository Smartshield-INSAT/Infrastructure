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
import threading
import packet_analyzer
import gzip
import shutil

# Configuration
RABBITMQ_URL = "amqp://guest:guest@localhost:5672"  # Default port for RabbitMQ is 5672
QUEUE_NAME = "testQueue"
API_NB15 = "http://192.168.100.88:8002/predict-all"

def run_preprocessing(script_name, filename):
    """Runs a preprocessing script with the provided data."""
    res = subprocess.run([f"./Logs-Extraction/ARGZEEK/{script_name}", filename], check=True)  # Run the script
    return res

def decompress_file(input_filename, output_filename):
    try:
        with gzip.open(input_filename, 'rb') as f_in:
            with open(output_filename, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"File decompressed successfully to {output_filename}")
    except Exception as e:
        print(f"Error while decompressing file: {e}")


def convert_to_parquet(filename, timestamp, id_srv):
    df= pd.read_csv(filename)
    new_file = f"{id_srv}_{timestamp}.parquet"

    with open(new_file, "wb") as f:
        df.to_parquet(f)

    return new_file


async def process_message(message):
    """Processes a single message by sending a file to the API."""
    
    # Handle new PCAP file
    id_srv= message.get('id')
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    filename= f"/pcap_files/{id_srv}_{timestamp}.pcap"
    
    try:
        # Decode the Base64 encoded compressed data
        compressed_bytes = base64.b64decode(compressed_data)
        compressed_filename = '/compressed_pcap/temp.pcap.gz'
        # Decompress the data (assuming gzip compression)
        with gzip.GzipFile(fileobj=open(compressed_filename, 'wb')) as gz:
            gz.write(compressed_bytes)

        print(f"Processed file with ID: {id_srv}")
        
        decompress_file(compressed_filename, filename)
        
    except Exception as e:
        print(f"Failed to process message: {e}")


    
    analyzer= PcapAnalyzer(filename)
    thread=threading.Thread(target=analyzer.get_json)
    thread.start()

    # Start Extraction
    run_preprocessing("process_pcap.sh", filename)

    # Wait for the output file
    while True:
        try:
            filename = f"Logs-Extraction/ARGZEEK/FinalOutput/MERGERD_{id_srv}_{timestamp}.csv"
            process_file(filename)
            break
        except:
            pass

    filename = convert_to_parquet(filename)

    with open(filename, "rb") as file:
        parquet_buffer = BytesIO(file.read())
    
    async with httpx.AsyncClient() as client:
        files = {'file': (filename, parquet_buffer, "application/octet-stream")}
        response = await client.post(API_NB15, files=files)

    thread.join()

    return response.text


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
        message = json.loads(body)

        res_nb15 = await process_message(message)
        print(res_nb15, '\n', res_cic)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Start consuming messages
    for method_frame, properties, body in channel.consume(queue=QUEUE_NAME, auto_ack=False):
        await callback(channel, method_frame, properties, body)



def main():

    # Start consuming messages
    asyncio.run(consume_messages())


if __name__ == "__main__":
    main()
