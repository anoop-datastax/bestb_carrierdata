from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
from dotenv import load_dotenv
import os
import json

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

# Prepare the statement for inserting data
insert_stmt = session.prepare("""
    INSERT INTO carrier_rates2 (destination, origin, billable_weight, carrier_data)
    VALUES (?, ?, ?, ?)
""")

# Function to read and process CSV file
def load_data_from_csv(file_path):
    df = pd.read_csv(file_path, skiprows=2)  # Adjust parameters as needed
    # Clean up column names
    df.columns = df.columns.str.strip().str.replace('"', '')
    # Print the first two rows for verification
    print(df.head(4))

    for _, row in df.iterrows():
        destination = row['Destination']
        origin = row['Origin']
        billable_weight = row['Billable Weight']
        carrier_data = {}  # Initialize empty dict for carrier_data

        # Assuming columns after 'Billable Weight' are carriers
        for carrier in df.columns[3:]:
            carrier_data[carrier] = row[carrier]  # Store data directly; consider converting to proper format if needed

        yield destination, origin, billable_weight, carrier_data

# Load and insert data
for destination, origin, billable_weight, carrier_data in load_data_from_csv('/Users/anooptiwari/Downloads/carrier_table_utf8.csv'):
    destination_str = str(destination)
    origin_str = str(origin)
    billable_weight_str = str(billable_weight)
    carrier_data_dict = {k: v for k, v in carrier_data.items()}
    session.execute(insert_stmt, (destination_str, origin_str, billable_weight_str, carrier_data_dict))

print("Data loading completed.")

#read data
import json

# Adjust the query to match what works in cqlsh
query = "SELECT carrier_data FROM carrier_rates2 WHERE destination = '61701' AND billable_weight = '\"1-25\"';"

result = session.execute(query)

for row in result:
    carrier_data = row.carrier_data
    # Assuming carrier_data is a dict with carrier keys as keys and JSON strings as values
    for key, json_str in carrier_data.items():
        try:
            # Parse the JSON string
            data = json.loads(json_str)
            print(f"Data for {key}: {data}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for key {key}: {e}")



# Now that both loading and querying are done, shutdown session and cluster connection
session.shutdown()
cluster.shutdown()
