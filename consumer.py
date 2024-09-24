# consumer.py

from kafka import KafkaConsumer
import gzip
import json
import pandas as pd

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'traffic_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Start reading at the earliest message in the topic
    enable_auto_commit=True,  # Automatically commit offsets
    group_id='traffic-consumer-group'  # Group ID for managing offsets
)

def consume_traffic_data():
    print("Starting to consume messages...")
    for message in consumer:
        try:
            # Attempt to decompress the message
            try:
                decompressed_data = gzip.decompress(message.value).decode('utf-8')
            except gzip.BadGzipFile:
                # If not gzipped, assume it's plain JSON
                decompressed_data = message.value.decode('utf-8')
            
            data = json.loads(decompressed_data)
            
            # Extract the data
            time_step = data['time_step']
            location_data_train = data['location_data_train']
            features_train = data['features_train']
            location_data_test = data.get('location_data_test', [])  # Handle missing test data gracefully
            features_test = data.get('features_test', [])  # Handle missing test data gracefully
            adjacency_matrix = data.get('adjacency_matrix', [])

            # Convert the received lists back to DataFrames for further processing if needed
            df_location_data_train = pd.DataFrame(location_data_train)
            df_features_train = pd.DataFrame(features_train)
            df_location_data_test = pd.DataFrame(location_data_test)
            df_features_test = pd.DataFrame(features_test)
            df_adjacency_matrix = pd.DataFrame(adjacency_matrix)

            print(f"Received data for time step {time_step} with {df_location_data_train.shape[0]} locations (train)")
            print(f"Location Data Train Head:\n{df_location_data_train.head()}")
            print(f"Features Data Train Head:\n{df_features_train.head()}")
            print(f"Location Data Test Head:\n{df_location_data_test.head()}")
            print(f"Features Data Test Head:\n{df_features_test.head()}")
            print(f"Adjacency Matrix Head:\n{df_adjacency_matrix.head()}")

        except Exception as e:
            print(f"Failed to process message: {e}")

if __name__ == "__main__":
    consume_traffic_data()
    print("Data consumption complete.")
