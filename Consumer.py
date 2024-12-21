import pandas as pd
import logging
from confluent_kafka import Consumer
import boto3
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('consumer.log'),
        logging.StreamHandler()
    ]
)

# Initialize MinIO Client
minio_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',  # Adjust your MinIO endpoint
    aws_access_key_id='minioadmin',       # MinIO Access Key
    aws_secret_access_key='minioadmin'    # MinIO Secret Key
)

# Configure Kafka Consumer
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe(['ebay'])  # Your Kafka topic

def initialize_new_batch(batch_number):
    """Initialize a new batch file on MinIO."""
    object_key = f"batch_{batch_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    try:
        minio_client.put_object(
            Bucket='ebay',  # Name of your MinIO bucket
            Key=object_key,  # Create an empty file for the new batch
            Body=''.encode('utf-8'),
            ContentLength=0
        )
        logging.info(f"New batch initialized: {object_key}")
        return object_key
    except Exception as e:
        logging.error(f"Failed to initialize new batch on MinIO: {e}")
        raise

def append_to_minio_batch(object_key, record):
    """Append a single record to an existing batch file on MinIO."""
    csv_record = pd.DataFrame([record], columns=[
        'b2c_c2c', 'seller_id', 'declared_handling_days',
        'shipment_method_id', 'item_zip', 'buyer_zip',
        'category_id', 'item_price', 'quantity', 'weight',
        'package_size', 'zip_distance', 'shipping_fee',
        'delivery_date', 'acceptance_date', 'timestamp'
    ]).to_csv(index=False, header=False)

    try:
        # Get the existing file from MinIO
        existing_data = minio_client.get_object(Bucket='ebay', Key=object_key)['Body'].read().decode('utf-8')
        # Append the new record to the existing data
        updated_data = existing_data + csv_record

        # Overwrite the file on MinIO with the updated data
        minio_client.put_object(
            Bucket='ebay',
            Key=object_key,
            Body=updated_data.encode('utf-8'),
            ContentLength=len(updated_data.encode('utf-8'))
        )
        logging.info(f"Record appended to batch: {object_key}")
    except Exception as e:
        logging.error(f"Failed to append to MinIO batch: {e}")

# Initialize variables
batch_size_limit = 25000
batch_number = 1
current_batch_size = 0
current_batch_key = initialize_new_batch(batch_number)

try:
    while True:
        message = consumer.poll(1.0)  # Poll with a 1-second timeout
        if message is None:
            continue
        if message.error():
            logging.error(f"Consumer error: {message.error()}")
            continue

        # Retrieve data from Kafka
        try:
            csv_data = message.value().decode('utf-8')
            record = csv_data.split(',')  # Split data into a list

            # Check the number of columns
            expected_columns = 16  # Adjust this according to your producer's output
            if len(record) != expected_columns:
                logging.error(f"Received record has {len(record)} columns, expected {expected_columns}. Data: {record}")
                continue  # Skip this record

            # Append the new record to the current batch on MinIO
            append_to_minio_batch(current_batch_key, record)
            current_batch_size += 1

            # Check if the current batch has reached the size limit
            if current_batch_size >= batch_size_limit:
                logging.info(f"Batch {batch_number} on MinIO is full with {current_batch_size} records. Finalizing...")
                batch_number += 1  # Increment the batch number
                current_batch_size = 0  # Reset the batch size
                current_batch_key = initialize_new_batch(batch_number)  # Start a new batch

        except Exception as e:
            logging.error(f"Error processing message: {e}")

except KeyboardInterrupt:
    logging.info("Consumer interrupted by user.")
finally:
    consumer.close()
    logging.info("Consumer closed.")
