import json
from astrapy.db import AstraDB
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Environment variables
# client_id = os.getenv('ASTRA_DB_CLIENT_ID')
# client_secret = os.getenv('ASTRA_DB_CLIENT_SECRET')
# secure_bundle_path = os.getenv('ASTRA_DB_SECURE_BUNDLE_PATH')
# keyspace_name = os.getenv('KEYSPACE_NAME')
namespace = os.getenv('NAMESPACE')
Token = os.getenv('TOKEN')
api_endpoint = os.getenv('API_ENDPOINT')



# Create a new AstraDB instance with a specified namespace
db = AstraDB(token=Token, api_endpoint=api_endpoint, namespace=namespace)

# Create a new AstraDB instance with the default namespace
# db = AstraDB(token="TOKEN", api_endpoint="API_ENDPOINT")


# Specify your collection name here
collection_name = 'carrier_rates_test'

collection = db.collection(collection_name)

# Load your JSON file
with open('/Users/anooptiwari/Documents/supply_chain_bb/one_row.json', 'r') as file:
    document = json.load(file)
    print(document)

# Insert the document into your collection
response = collection.insert_one(document)
#print(response)