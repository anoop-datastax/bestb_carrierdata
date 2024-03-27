import pandas as pd
import json

# Load the original CSV file
df = pd.read_csv('/Users/anooptiwari/Downloads/carrier_table_utf8.csv', dtype=str)

# Create an empty DataFrame for the transformed data
transformed_df = pd.DataFrame(columns=['Destination', 'Billable Weight', 'Carrier Type', 'Carrier Index', 'Carrier JSON', 'Origin'])

# Process each row in the original DataFrame
for index, row in df.iterrows():
    # For each carrier column in the row
    for carrier_col in ['Carrier1', 'Carrier2', 'CarrierN']:  # Update these names based on your actual CSV column names
        # Check if the carrier data is present and is not NaN
        if carrier_col in row and pd.notna(row[carrier_col]):
            # Load the carrier data as a JSON object
            try:
                carrier_data = json.loads(row[carrier_col])
                # For each item in the carrier series data
                for item in carrier_data['carrSer']:
                    # Extract carrier index and JSON string
                    carrier_index = item['type']
                    carrier_json = json.dumps(item)
                    # Append the transformed data to the new DataFrame
                    transformed_df = transformed_df.append({
                        'Destination': row['Destination'],
                        'Billable Weight': row['Billable Weight'],
                        'Carrier Type': carrier_col,
                        'Carrier Index': carrier_index,
                        'Carrier JSON': carrier_json,
                        'Origin': row['Origin']
                    }, ignore_index=True)
            except json.JSONDecodeError:
                print(f"Error decoding JSON for row {index}, column {carrier_col}")

# Save the transformed data to a new CSV file
transformed_df.to_csv('/Users/anooptiwari/Downloads/transformed_carrier_table_utf8.csv', index=False, encoding='utf-8')
