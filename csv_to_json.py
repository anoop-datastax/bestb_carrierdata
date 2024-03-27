import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Assuming your CSV file is named 'carrier_table_utf8.csv' and is located in a specific directory
# Update the file path according to where your UTF-8 encoded CSV file is located
file_path = 'path/to/your/carrier_table_utf8.csv'

def convert_csv_to_json(file_path):
    # Read the CSV file without headers to manually set them later
    data = pd.read_csv(file_path, encoding='utf-8', header=None,
                       skiprows=3)  # Skipping the first row if it's empty or not needed

    # Manually setting the column names
    column_names = ['Destination', 'Billable Weight', 'Origin', 'Carrier1', 'Carrier 2', 'Carrier n']
    data.columns = column_names

    # No need to reset index here since we're manually setting column names and skipping the appropriate rows

    # Convert each row to a JSON object
    json_objects = data.apply(lambda x: x.to_json(), axis=1)

    # Define the path for the output JSON file
    output_file_path = os.path.join(os.getcwd(), 'output.json')

    # Write the JSON objects to an output file
    with open(output_file_path, 'w') as f_out:
        for json_obj in json_objects:
            f_out.write(json_obj + '\n')

    print(f'JSON objects have been successfully written to {output_file_path}')


if __name__ == "__main__":
    file_path='/Users/anooptiwari/Downloads/carrier_table_utf8.csv'
    convert_csv_to_json(file_path)
