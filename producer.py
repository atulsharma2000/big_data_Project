from kafka import KafkaProducer
import pandas as pd
import json

# Producer will (Send Data to Kafka topic)
# Since the CSV file is large, I will be using pandas to read it in chunks and stream it to Kafka.

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

csv_file = "../data.csv"
chunk_size = 1000  # Adjust based on system performance

for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
    for _, row in chunk.iterrows():
        data = row.to_dict()
        producer.send("news_topic", value=data)
    print(f"Sent {chunk_size} records to Kafka")

producer.flush()
producer.close()
