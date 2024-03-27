import json
from astrapy.db import AstraDB
from dotenv import load_dotenv
import os

# Function to split string into chunks by byte size
def split_by_byte_limit(s, limit=8000):
    chunks = []
    while s:
        chunk_size = min(len(s), limit)
        chunks.append(s[:chunk_size])
        s = s[chunk_size:]
    return chunks

# Load environment variables
load_dotenv()

# Retrieve environment variables
namespace = os.getenv('NAMESPACE')
Token = os.getenv('TOKEN')
api_endpoint = os.getenv('API_ENDPOINT')

# Initialize AstraDB instance
db = AstraDB(token=Token, api_endpoint=api_endpoint, namespace=namespace)

# Collection name
collection_name = 'best_buy'
collection = db.collection(collection_name)

# Path to your JSON file
json_file_path = '/Users/anooptiwari/Documents/supply_chain_bb/output.json'

# Read and process JSON data
with open(json_file_path, 'r', encoding='utf-8') as file:
    data = json.load(file)
    # Assuming 'Carrier1', 'Carrier2', ..., 'CarrierN' need to be split
    for key in ['Carrier1', 'Carrier2', 'CarrierN']:  # Adjust based on your keys
        if key in data:
            data[key] = split_by_byte_limit(data[key], 8000)

    # Insert processed data into AstraDB
    #response = collection.insert_many([data])
    response = collection.insert_many([data])
    print(response)
