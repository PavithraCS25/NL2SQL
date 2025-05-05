import pandas as pd
import numpy as np
import datetime
import pandas_gbq # For easy DataFrame to BigQuery loading
import warnings
import os
import uuid
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
# (Keep your existing configuration section)
GCP_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")
STORES_TABLE_ID = os.getenv("STORES_TABLE_ID", "stores")
PRODUCTS_TABLE_ID = os.getenv("PRODUCTS_TABLE_ID", "products")
SALES_TABLE_ID = os.getenv("SALES_TABLE_ID", "sales_transactions")

# --- Simulation Parameters ---
# (Keep your existing parameters section)
start_date = datetime.date(2023, 1, 1)
end_date = datetime.date(2024, 12, 31)
num_transactions_per_day_min = 50
num_transactions_per_day_max = 250
max_quantity_per_transaction = 5

if not GCP_PROJECT_ID or not BQ_DATASET_ID:
    raise ValueError("Error: GCP_PROJECT_ID and BQ_DATASET_ID environment variables must be set.")

print(f"--- Configuration ---")
print(f"GCP Project ID: {GCP_PROJECT_ID}")
print(f"BigQuery Dataset ID: {BQ_DATASET_ID}")
print(f"Stores Table: {STORES_TABLE_ID}")
print(f"Products Table: {PRODUCTS_TABLE_ID}")
print(f"Sales Table: {SALES_TABLE_ID}")
print(f"Date Range: {start_date} to {end_date}")
print("---------------------")

# --- NEW: Helper Function: Get COMPANY Financial Year ---
def get_company_fy(date_obj):
    """Calculates the COMPANY Financial Year (Sep 1 - Aug 31) for a given date."""
    # Ensure we have a date or datetime object
    if isinstance(date_obj, pd.Timestamp):
        date_obj = date_obj.date() # Extract date part if it's a Pandas Timestamp
    elif isinstance(date_obj, datetime.datetime):
         date_obj = date_obj.date() # Extract date part if it's a standard datetime

    if not isinstance(date_obj, datetime.date):
        return None # Or raise an error if preferred

    year = date_obj.year
    month = date_obj.month
    if month >= 9:  # September or later
        fy_year_end = year + 1
    else:  # January to August
        fy_year_end = year
    return f"FY{str(fy_year_end)[-2:]}" # Format as FYYY (e.g., FY23)

# --- 1. Generate Stores Data ---
# (Keep existing stores generation code)
print("Generating Stores data...")
stores_data = [
    {'store_id': 'ST001', 'store_name': 'Tampines', 'city': 'Singapore', 'country': 'Singapore', 'opening_date': datetime.date(2010, 11, 1)},
    {'store_id': 'ST002', 'store_name': 'Alexandra', 'city': 'Singapore', 'country': 'Singapore', 'opening_date': datetime.date(2015, 5, 15)},
    {'store_id': 'ST003', 'store_name': 'Jurong', 'city': 'Singapore', 'country': 'Singapore', 'opening_date': datetime.date(2021, 4, 29)},
    {'store_id': 'ST004', 'store_name': 'Batu Kawan', 'city': 'Penang', 'country': 'Malaysia', 'opening_date': datetime.date(2019, 3, 14)},
    {'store_id': 'ST005', 'store_name': 'Cheras', 'city': 'Kuala Lumpur', 'country': 'Malaysia', 'opening_date': datetime.date(2015, 11, 19)},
]
df_stores = pd.DataFrame(stores_data)
df_stores['opening_date'] = pd.to_datetime(df_stores['opening_date']).dt.date
print(f"Generated {len(df_stores)} stores.")
# print(df_stores.head()) # Optional print

# --- 2. Generate Products Data ---
# (Keep existing products generation code)
print("\nGenerating Products data...")
products_data = [
    {"product_name": "Bookcase", "category": "Bookcases", "price": 79.90},
    {"product_name": "Wardrobe", "category": "Wardrobes", "price": 450.00},
    {"product_name": "Shelving Unit", "category": "Shelving Units", "price": 69.00},
    {"product_name": "Bed Frame", "category": "Beds", "price": 299.00},
    {"product_name": "Sofa", "category": "Sofas", "price": 599.00},
    {"product_name": "Armchair", "category": "Armchairs", "price": 129.00},
    {"product_name": "Side Table", "category": "Side Tables", "price": 19.90},
    {"product_name": "Chests of Drawers", "category": "Chests of Drawers", "price": 249.00},
    {"product_name": "Sofa-beds", "category": "Sofa-beds", "price": 699.00},
    {"product_name": "Wing Chair", "category": "Armchairs", "price": 349.00},
    {"product_name": "Step Stool", "category": "Step Stools", "price": 29.90},
    {"product_name": "Desk Accessories", "category": "Desk Accessories", "price": 9.90},
    {"product_name": "Mug", "category": "Tableware", "price": 1.50},
    {"product_name": "Plastic Bag", "category": "Food Storage", "price": 3.90},
    {"product_name": "Clothes Storage", "category": "Clothes Storage", "price": 24.90}
]
df_products = pd.DataFrame(products_data)
df_products['product_id'] = [f"P{str(i+1).zfill(4)}" for i in range(len(df_products))]
df_products = df_products[['product_id', 'product_name', 'category', 'price']]
df_products['price'] = df_products['price'].astype(float)
product_price_map = df_products.set_index('product_id')['price'].to_dict()
print(f"Generated {len(df_products)} products.")
# print(df_products.head()) # Optional print

# --- 3. Generate Sales Transactions Data ---
print("\nGenerating Sales Transactions data...")
# (Keep existing sales generation logic up to creating the DataFrame)
all_transactions = []
dates = pd.date_range(start=start_date, end=end_date, freq='D')
store_ids = df_stores['store_id'].tolist()
product_ids = df_products['product_id'].tolist()
num_days = len(dates)
total_expected_transactions_approx = num_days * ((num_transactions_per_day_min + num_transactions_per_day_max) // 2)
print(f"Generating approximately {total_expected_transactions_approx} transactions across {num_days} days...")
num_transactions_per_day = np.random.randint(num_transactions_per_day_min, num_transactions_per_day_max + 1, size=num_days)
total_transactions = sum(num_transactions_per_day)
random_store_indices = np.random.randint(0, len(store_ids), size=total_transactions)
random_product_indices = np.random.randint(0, len(product_ids), size=total_transactions)
random_quantities = np.random.randint(1, max_quantity_per_transaction + 1, size=total_transactions)
random_seconds_in_day = np.random.randint(0, 86400, size=total_transactions)
current_transaction_index = 0
for i, date in enumerate(dates):
    num_transactions = num_transactions_per_day[i]
    day_start_dt = datetime.datetime.combine(date, datetime.time.min)
    if (i + 1) % 50 == 0:
        print(f"  Generating for day {i+1}/{num_days} ({date})...")
    transactions_for_day = []
    for _ in range(num_transactions):
        sales_id = str(uuid.uuid4())
        store_id = store_ids[random_store_indices[current_transaction_index]]
        product_id = product_ids[random_product_indices[current_transaction_index]]
        quantity = random_quantities[current_transaction_index]
        price_at_sale = product_price_map[product_id]
        total_amount = round(quantity * price_at_sale, 2)
        transaction_dt = day_start_dt + datetime.timedelta(seconds=int(random_seconds_in_day[current_transaction_index]))
        transactions_for_day.append({
            'sales_id': sales_id,
            'store_id': store_id,
            'product_id': product_id,
            'sale_date': transaction_dt, # Timestamp object
            'quantity': quantity,
            'price_at_sale':price_at_sale,
            'total_amount': total_amount,
        })
        current_transaction_index += 1
    all_transactions.extend(transactions_for_day)

df_sales = pd.DataFrame(all_transactions)

# *** ADD THE FY COLUMN HERE ***
print("Calculating Financial Year (FY) for transactions...")
df_sales['FY'] = df_sales['sale_date'].apply(get_company_fy)
# *** ---------------------- ***

# Ensure correct data types before loading
df_sales['quantity'] = df_sales['quantity'].astype(int)
df_sales['price_at_sale'] = df_sales['price_at_sale'].astype(float)
df_sales['total_amount'] = df_sales['total_amount'].astype(float)

print(f"Generated {len(df_sales)} sales transactions.")
print("\nSample Sales Data (with FY):")
# Print head including the new FY column
print(df_sales[['sale_date', 'FY', 'store_id', 'product_id', 'quantity','price_at_sale', 'total_amount']].head())


# --- 4. Load Data to BigQuery ---

warnings.filterwarnings("ignore", category=FutureWarning, module="pandas_gbq")

# Define target tables and schemas
tables_to_load = {
    "Stores": {
        # (Keep Stores config as before)
        "dataframe": df_stores,
        "table_id": f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{STORES_TABLE_ID}",
        "schema": [
            {'name': 'store_id', 'type': 'STRING'},
            {'name': 'store_name', 'type': 'STRING'},
            {'name': 'city', 'type': 'STRING'},
            {'name': 'country', 'type': 'STRING'},
            {'name': 'opening_date', 'type': 'DATE'},
        ]
    },
    "Products": {
         # (Keep Products config as before)
        "dataframe": df_products,
        "table_id": f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{PRODUCTS_TABLE_ID}",
        "schema": [
            {'name': 'product_id', 'type': 'STRING'},
            {'name': 'product_name', 'type': 'STRING'},
            {'name': 'category', 'type': 'STRING'},
            {'name': 'price', 'type': 'FLOAT'}, # Using FLOAT as decided earlier
        ]
    },
    "Sales Transactions": {
        "dataframe": df_sales, # Use the updated df_sales
        "table_id": f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{SALES_TABLE_ID}",
        # *** UPDATED SCHEMA TO INCLUDE FY ***
        "schema": [
            {'name': 'sales_id', 'type': 'STRING'},
            {'name': 'store_id', 'type': 'STRING'},
            {'name': 'product_id', 'type': 'STRING'},
            {'name': 'sale_date', 'type': 'TIMESTAMP'},
            {'name': 'FY', 'type': 'STRING'}, # Added FY column schema
            {'name': 'quantity', 'type': 'INTEGER'},
            {'name': 'price_at_sale', 'type': 'FLOAT'},
            {'name': 'total_amount', 'type': 'FLOAT'}, # Using FLOAT as decided earlier
        ]
        # *** ----------------------------- ***
    }
}

# (Keep the load_table_to_bq function and the loading loop as before)
def load_table_to_bq(table_name, config):
    print(f"\nAttempting to load data into BigQuery table: {config['table_id']}")
    try:
        # Reorder DataFrame columns to match schema - good practice
        schema_columns = [col['name'] for col in config['schema']]
        df_to_load = config['dataframe'][schema_columns]

        pandas_gbq.to_gbq(
            df_to_load, # Use reordered DataFrame
            destination_table=config['table_id'],
            project_id=GCP_PROJECT_ID,
            if_exists='replace',
            table_schema=config['schema'],
            progress_bar=True
        )
        print(f"Successfully loaded {table_name} data into {config['table_id']}")
        return True
    except Exception as e:
        print(f"\nError loading {table_name} data to BigQuery: {e}")
        if "Could not convert DataFrame to Parquet" in str(e):
             print("This often relates to data type incompatibilities (e.g., try FLOAT instead of NUMERIC in schema) or problematic values (NaNs).")
             print("Check DataFrame info:")
             try:
                 config['dataframe'].info()
                 print("Null values:\n", config['dataframe'].isnull().sum())
             except Exception as inspect_e:
                 print(f"Could not inspect DataFrame: {inspect_e}")
        print("Please also check:")
        print(f"1. If GCP_PROJECT_ID ('{GCP_PROJECT_ID}') and BQ_DATASET_ID ('{BQ_DATASET_ID}') are correct.")
        print(f"2. If the dataset '{BQ_DATASET_ID}' exists in project '{GCP_PROJECT_ID}'.")
        print(f"3. Your authentication credentials (e.g., run 'gcloud auth application-default login').")
        print(f"4. If the service account has 'BigQuery Data Editor' role on the dataset '{BQ_DATASET_ID}'.")
        return False

success_count = 0
for name, cfg in tables_to_load.items():
    if load_table_to_bq(name, cfg):
        success_count += 1

print(f"\n--- Load Summary ---")
print(f"Successfully loaded {success_count} out of {len(tables_to_load)} tables.")
print("--------------------")