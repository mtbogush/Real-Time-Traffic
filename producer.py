# producer.py

import pickle
from kafka import KafkaProducer
import json
import time
import gzip
import sys
import pandas as pd
from scipy.sparse import csr_matrix

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Load the pickled data
with open('tra_X_tr.pkl', 'rb') as f:
    tra_X_tr = pickle.load(f)

with open('tra_Y_tr.pkl', 'rb') as f:
    tra_Y_tr = pickle.load(f)

with open('tra_X_te.pkl', 'rb') as f:
    tra_X_te = pickle.load(f)

with open('tra_Y_te.pkl', 'rb') as f:
    tra_Y_te = pickle.load(f)

with open('tra_adj_mat.pkl', 'rb') as f:
    tra_adj_mat = pickle.load(f)

# Convert to pandas DataFrames for easier handling
df_tra_X_tr = pd.DataFrame(tra_X_tr[0][0].toarray())  # Convert sparse matrix to dense
df_tra_Y_tr = pd.DataFrame(tra_Y_tr[:, 0])
df_tra_X_te = pd.DataFrame(tra_X_te[0][0].toarray())  # Convert sparse matrix to dense
df_tra_Y_te = pd.DataFrame(tra_Y_te[:, 0])
df_adj_mat = pd.DataFrame(tra_adj_mat)  # Adjacency matrix as DataFrame for demonstration

# Define the chunk size (e.g., split data into groups of 10 locations)
chunk_size = 10
num_locations = df_tra_Y_tr.shape[0]  # Should be 36

def send_traffic_data():
    # Loop through each time step in the training and testing data and send it to Kafka
    for i in range(df_tra_Y_tr.shape[1]):
        for start in range(0, num_locations, chunk_size):
            end = min(start + chunk_size, num_locations)
            # Example data payload for a subset of locations
            data = {
                'time_step': i,
                'location_data_train': df_tra_Y_tr.iloc[start:end, i].values.tolist(),  # Training location data
                'features_train': df_tra_X_tr.iloc[start:end, :].values.tolist(),  # Training features
                'location_data_test': df_tra_Y_te.iloc[start:end, i].values.tolist() if i < df_tra_Y_te.shape[1] else [],  # Test location data
                'features_test': df_tra_X_te.iloc[start:end, :].values.tolist() if i < df_tra_X_te.shape[1] else [],  # Test features
                'adjacency_matrix': df_adj_mat.values.tolist()  # Adjacency matrix
            }
            # Compress the data before sending
            compressed_data = gzip.compress(json.dumps(data).encode('utf-8'))

            # Check the size of the compressed data
            size_of_message = sys.getsizeof(compressed_data)
            print(f"Size of compressed message at time step {i}, locations {start}-{end}: {size_of_message} bytes")

            # Ensure the size does not exceed 1 GB (or other appropriate limit)
            if size_of_message > 1073741824:  # 1 GB in bytes
                print("Warning: Message size exceeds 1 GB. Consider reducing chunk size or further compressing data.")
                continue
            
            # Send compressed data to Kafka topic 'traffic_data'
            producer.send('traffic_data', compressed_data)
            time.sleep(0.1)  # Adjust delay as needed for smaller chunks

if __name__ == "__main__":
    send_traffic_data()
    print("Data sending complete.")
