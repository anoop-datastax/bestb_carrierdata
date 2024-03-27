from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import json
import ast  # Import ast for safely evaluating strings as Python literals
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Astra DB connection parameters
cloud_config = {
    'secure_connect_bundle': os.getenv('ASTRA_DB_SECURE_BUNDLE_PATH')
}
auth_provider = PlainTextAuthProvider(os.getenv('ASTRA_DB_CLIENT_ID'), os.getenv('ASTRA_DB_CLIENT_SECRET'))
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# Set keyspace
session.set_keyspace(os.getenv('KEYSPACE_NAME'))

# Function to insert data into Astra DB
def insert_data(destination, origin, weight, carrier_type, carrier_index, carrier_json):
    query = """
    INSERT INTO best_buy.carrier_rates3 (destination, origin, weight, carrier_type, carrier_index, carrier_json)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (destination, origin, weight, carrier_type, carrier_index, carrier_json))

# Function to read and process the Excel file
def process_excel_file(excel_path):
    df = pd.read_excel(excel_path)
    df.columns = df.columns.str.strip().str.replace('"', '')

    for _, row in df.iterrows():
        destination = str(row['Destination']) # Convert to string if not already
        weight = str(row['Billable Weight'])  # Convert to string if not already
        origin = str(row['Origin'])  # Explicitly convert to string

        # Iterate over carrier columns, which are assumed to be after the first 3 columns
        for column in df.columns[3:]:
            carrier_type = column
            carrier_data_str = row[column]

            # Skip rows where carrier data might be NaN or empty
            if pd.isna(carrier_data_str):
                continue

            try:
                # Safely evaluate the string as a Python dictionary
                outer_dict = ast.literal_eval(carrier_data_str)

                # Iterate through the dictionary, parsing each JSON string
                for carrier_index, json_str in outer_dict.items():
                    inner_json = json.loads(json_str.replace("'", "\""))
                    insert_data(destination, origin, weight, carrier_type, int(carrier_index), json.dumps(inner_json))

            except (ValueError, json.JSONDecodeError) as e:
                print(f"Error parsing data in column '{column}': {e}")
                print(f"Data causing error: {carrier_data_str}")

if __name__ == "__main__":
    excel_path = '/Users/anooptiwari/Downloads/carrier_data_modified.xlsx'  # Update with the actual path to your Excel file
    process_excel_file(excel_path)
    # Close the Astra DB connection
    cluster.shutdown()
