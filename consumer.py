from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)

# kafka Consumer Configuration
consumer = KafkaConsumer(
    'news_topic',  # topic to subscribe to
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# HDFS Configuration
hdfs_client = InsecureClient('http://localhost:9870', user='atul')  
hdfs_base_path = '/user/atul/news_data/input/'  # base path where the data will be stored

# write data to HDFS in batches
def write_to_hdfs(batch_data, file_index):
    hdfs_path = os.path.join(hdfs_base_path, f'news_data_{file_index}.json')  # Unique file name for each batch
    try:
        with hdfs_client.write(hdfs_path, overwrite=True) as writer:
            for record in batch_data:
                writer.write(json.dumps(record) + '\n')  # writing each record as a new line
        logging.info(f"Saved {len(batch_data)} records to HDFS at {hdfs_path}.")
    except Exception as e:
        logging.error(f"Error writing to HDFS: {e}")


# Consuming messages from Kafka and saving them to HDFS
batch_size = 1000  # Number of records to write in one batch
batch_data = []
record_count = 0
file_index = 1  # To keep track of the file number

try:
    for message in consumer:
        data = message.value  # Get the value of the message
        batch_data.append(data)  # Add data to batch
        record_count += 1
        
        # Write to HDFS if batch size is reached
        if len(batch_data) >= batch_size:
            write_to_hdfs(batch_data, file_index)
            batch_data.clear()  # Clear batch after writing
            file_index += 1  # Increment file index for next batch

except KeyboardInterrupt:
    logging.info("Consumer stopped.")
finally:
    if batch_data:  # write any remaining records in the last batch
        write_to_hdfs(batch_data, file_index)
    
    logging.info(f"Total records consumed: {record_count}")
    consumer.close()
