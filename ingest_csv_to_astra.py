from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import json
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# ASTRA_CLIENT_ID = os.getenv('ASTRA_CLIENT_ID')
# ASTRA_CLIENT_SECRET = os.getenv('ASTRA_CLIENT_SECRET')
# ASTRA_SECURE_CONNECT_BUNDLE_PATH = os.getenv('ASTRA_SECURE_CONNECT_BUNDLE_PATH')
# KEYSPACE = os.getenv('KEYSPACE')
#
# # Initialize Astra DB connection
# auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET)
# cluster = Cluster(cloud={'secure_connect_bundle': ASTRA_SECURE_CONNECT_BUNDLE_PATH}, auth_provider=auth_provider)
# session = cluster.connect(KEYSPACE)

# Astra DB connection parameters
cloud_config = {
    'secure_connect_bundle': os.getenv('ASTRA_DB_SECURE_BUNDLE_PATH')
}
auth_provider = PlainTextAuthProvider(os.getenv('ASTRA_DB_CLIENT_ID'), os.getenv('ASTRA_DB_CLIENT_SECRET'))
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# Set keyspace
session.set_keyspace(os.getenv('KEYSPACE_NAME'))

# Read the Excel file
excel_path = '/Users/anooptiwari/Downloads/carrier_data_modified.xlsx'  # Update with the path to your Excel file
df = pd.read_excel(excel_path)
#df.columns = df.columns.str.strip().str.replace('"', '')

# Function to insert data into Astra DB
def insert_data(destination, origin, weight, carrier_type, carrier_index, carrier_json):
    query = """
    INSERT INTO carrier_rates3 (destination, origin, weight, carrier_type, carrier_index, carrier_json)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (destination, origin, weight, carrier_type, carrier_index, carrier_json))

# Iterate through the DataFrame and insert each row into Astra DB
###
for _, row in df.iterrows():
    destination = row['Destination']
    origin = row['Origin']
    billable_weight = str(row['Billable Weight'])  # Convert to string if not already

    # Iterate over carrier columns, which are assumed to be after the first 3 columns
    for column in df.columns[3:]:
        carrier_type = column  # This is your carrier type (e.g., carrier1, carrier2, etc.)
        carrier_json_str = row[column]  # The JSON string from the cell

        # Skip rows where carrier data might be NaN or empty
        if pd.isna(carrier_json_str):
            continue

        try:
            # Parse the JSON string; ensure it's correctly formatted for JSON
            carrier_data_parsed = json.loads(carrier_json_str.replace("'", "\""))


            # Assuming carrier_data_parsed is a dict with numeric_identifier as keys
            for carrier_index, carrier_info in carrier_data_parsed.items():
                # Corrected variable reference for carrier_info in json.dumps()
                insert_data(destination, origin, billable_weight, carrier_type, int(carrier_index), json.dumps(carrier_info))

        except json.JSONDecodeError as e:
            print(f"Error parsing JSON in column '{column}': {e}")
            print(f"Data causing error: {carrier_json_str}")

###



# Close the Astra DB connection
cluster.shutdown()
