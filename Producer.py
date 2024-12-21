import pandas as pd
import time
import logging
from datetime import datetime
from confluent_kafka import Producer
import os

# Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'linger.ms': 5,
    'batch.size': 16384,
    'compression.type': 'none',
    'acks': 'all'
}
KAFKA_TOPIC = 'ebay'
CSV_PATH = 'train_data.csv'
INDEX_FILE_PATH = 'last_processed_index.txt'

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('producer.log'),
        logging.StreamHandler()
    ]
)

def delivery_report(err, msg):
    """Log if there's an error sending a message to Kafka."""
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def create_producer():
    """Create Kafka producer."""
    logging.info("Creating Kafka producer...")
    return Producer(KAFKA_CONFIG)

def load_last_processed_index():
    """Load the last processed index from a file."""
    try:
        with open(INDEX_FILE_PATH, 'r') as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return 0  # Default to 0 if the file doesn't exist

def save_last_processed_index(index):
    """Save the last processed index to a file."""
    with open(INDEX_FILE_PATH, 'w') as f:
        f.write(str(index))

def stream_data(producer):
    """Stream data from file to Kafka topic."""
    last_modified_time = 0
    last_processed_index = load_last_processed_index()  # Load index on startup

    while True:
        try:
            current_modified_time = os.path.getmtime(CSV_PATH)
            if current_modified_time > last_modified_time:
                logging.info("New data detected. Reading CSV...")
                df = pd.read_csv(CSV_PATH)

                # Process new records only
                for index, row in df.iterrows():
                    if index < last_processed_index:
                        continue  # Skip already processed records

                    # Create data in dictionary format for Kafka
                    data = {
                        'b2c_c2c': str(row['b2c_c2c']),
                        'seller_id': str(row['seller_id']),
                        'declared_handling_days': float(row['declared_handling_days']),
                        'shipment_method_id': int(row['shipment_method_id']),
                        'item_zip': str(row['item_zip']),
                        'buyer_zip': str(row['buyer_zip']),
                        'category_id': int(row['category_id']),
                        'item_price': float(row['item_price']),
                        'quantity': int(row['quantity']),
                        'weight': float(row['weight']),
                        'package_size': str(row['package_size']),
                        'zip_distance': float(row['zip_distance']),
                        'shipping_fee': float(row['shipping_fee']),
                        'delivery_date': str(row['delivery_date']),
                        'acceptance_date': str(row['acceptance_date']),
                        'timestamp': datetime.now().isoformat()
                    }

                    # Send data to Kafka as CSV string
                    csv_row = ','.join([str(data[col]) for col in data])
                    producer.produce(
                        KAFKA_TOPIC,
                        key=str(index),
                        value=csv_row,
                        callback=delivery_report
                    )

                    # Mark record as processed
                    last_processed_index = index + 1  # Update last processed index

                    # Poll for delivery report
                    producer.poll(0)

                # Update last modified time after processing
                last_modified_time = current_modified_time

                # Flush producer after all messages are sent
                producer.flush()

                # Save the last processed index to file
                save_last_processed_index(last_processed_index)

            time.sleep(5)  # Wait before checking the file again

        except Exception as e:
            logging.error(f"Error while streaming data: {e}")

if __name__ == "__main__":
    producer = create_producer()
    try:
        stream_data(producer)
    finally:
        producer.flush()  # Ensure all messages are sent before exiting
        save_last_processed_index(last_processed_index)  # Save the last processed index