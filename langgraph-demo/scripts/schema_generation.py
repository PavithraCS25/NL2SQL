import json
from google.cloud import storage
import os # Optional: for environment variable based configuration
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
# Replace with your actual bucket name
BUCKET_NAME = os.getenv("BUCKET_NAME")
# Replace with the desired path and filename in the bucket
DESTINATION_BLOB_NAME = "schema_descriptions.json"
# --- End Configuration ---

# --- Paste the updated schema_descriptions list here ---
schema_descriptions = [
  {
    "id": "schema_table_stores_0", 
    "type": "table",
    "name": "stores",
    "description": "Table 'stores' contains information about company store locations. It includes unique store identifiers (store_id), the public name of the store (store_name), the city (city) and country (country) where it's located, and the date it opened (opening_date)."
  },
  {
    "id": "schema_table_products_1", 
    "type": "table",
    "name": "products",
    "description": "Table 'products' holds details about the items available for sale. It contains a unique identifier for each product (product_id), the product's name (product_name), its category (category), and its current standard selling price (price)."
  },
  {
    "id": "schema_table_sales_transactions_2", 
    "type": "table",
    "name": "sales_transactions",
    "description": "Table 'sales_transactions' records individual product sales events. Each row represents a specific product line item within a larger customer transaction. It includes a unique identifier for the sale line (sales_id), an identifier for the overall transaction (transaction_id), foreign keys linking to the store (store_id) and product (product_id) involved, the date of the sale (sale_date), the fiscal year of the sale (FY), the number of units sold (quantity), the price per unit at the time of sale (price_at_sale), and the total amount for that line item (total_amount)."
  },
  {
    "id": "schema_column_stores_store_id_3", 
    "type": "column",
    "table": "stores",
    "name": "store_id",
    "description": "Column 'store_id' in the 'stores' table is the unique identifier for each store location. It serves as the primary key for this table and can be used to link to the 'sales_transactions' table."
  },
  {
    "id": "schema_column_stores_store_name_4", 
    "type": "column",
    "table": "stores",
    "name": "store_name",
    "description": "Column 'store_name' in the 'stores' table holds the common, public name of the company store."
  },
  {
    "id": "schema_column_stores_city_5", 
    "type": "column",
    "table": "stores",
    "name": "city",
    "description": "Column 'city' in the 'stores' table indicates the city where the store is situated."
  },
  {
    "id": "schema_column_stores_country_6", 
    "type": "column",
    "table": "stores",
    "name": "country",
    "description": "Column 'country' in the 'stores' table specifies the country where the store is located."
  },
  {
    "id": "schema_column_stores_opening_date_7", 
    "type": "column",
    "table": "stores",
    "name": "opening_date",
    "description": "Column 'opening_date' in the 'stores' table records the date when the store officially opened to the public."
  },
  {
    "id": "schema_column_products_product_id_8", 
    "type": "column",
    "table": "products",
    "name": "product_id",
    "description": "Column 'product_id' in the 'products' table is the unique identifier for each product. It serves as the primary key for this table and can be used to link to the 'sales_transactions' table."
  },
  {
    "id": "schema_column_products_product_name_9", 
    "type": "column",
    "table": "products",
    "name": "product_name",
    "description": "Column 'product_name' in the 'products' table contains the commercial name of the product."
  },
  {
    "id": "schema_column_products_category_10", 
    "type": "column",
    "table": "products",
    "name": "category",
    "description": "Column 'category' in the 'products' table classifies the product into a specific group, such as 'Furniture', 'Kitchenware', or 'Textiles'."
  },
  {
    "id": "schema_column_products_price_11", 
    "type": "column",
    "table": "products",
    "name": "price",
    "description": "Column 'price' in the 'products' table represents the current standard selling price per unit of the product. Note that the actual price paid in a transaction is recorded in 'sales_transactions.price_at_sale'."
  },
  {
    "id": "schema_column_sales_transactions_sales_id_12", 
    "type": "column",
    "table": "sales_transactions",
    "name": "sales_id",
    "description": "Column 'sales_id' in the 'sales_transactions' table is the unique identifier for each specific product line item sold within a transaction. It serves as the primary key for this table."
  },
  {
    "id": "schema_column_sales_transactions_transaction_id_13", 
    "type": "column",
    "table": "sales_transactions",
    "name": "transaction_id",
    "description": "Column 'transaction_id' in the 'sales_transactions' table groups multiple sale line items belonging to the same customer purchase event (receipt)."
  },
  {
    "id": "schema_column_sales_transactions_store_id_14", 
    "type": "column",
    "table": "sales_transactions",
    "name": "store_id",
    "description": "Column 'store_id' in the 'sales_transactions' table is a foreign key referencing the 'stores' table's 'store_id', indicating which store location made the sale."
  },
  {
    "id": "schema_column_sales_transactions_product_id_15", 
    "type": "column",
    "table": "sales_transactions",
    "name": "product_id",
    "description": "Column 'product_id' in the 'sales_transactions' table is a foreign key referencing the 'products' table's 'product_id', identifying the specific product sold."
  },
  {
    "id": "schema_column_sales_transactions_sale_date_16", 
    "type": "column",
    "table": "sales_transactions",
    "name": "sale_date",
    "description": "Column 'sale_date' in the 'sales_transactions' table records the date (and potentially time) when the transaction occurred."
  },
  {
    "id": "schema_column_sales_transactions_FY_17", 
    "type": "column",
    "table": "sales_transactions",
    "name": "FY",
    "description": "Column 'FY' in the 'sales_transactions' table represents the fiscal year in which the sale occurred. This is often used for financial reporting and analysis and may be derived from the 'sale_date'."
  },
  {
    "id": "schema_column_sales_transactions_quantity_18", 
    "type": "column",
    "table": "sales_transactions",
    "name": "quantity",
    "description": "Column 'quantity' in the 'sales_transactions' table specifies the number of units of the 'product_id' sold in this specific line item."
  },
  {
    "id": "schema_column_sales_transactions_price_at_sale_19", 
    "type": "column",
    "table": "sales_transactions",
    "name": "price_at_sale",
    "description": "Column 'price_at_sale' in the 'sales_transactions' table records the price per unit of the product *at the time the sale was made*. This might differ from the current price in the 'products' table due to promotions or price changes."
  },
  {
    "id": "schema_column_sales_transactions_total_amount_20", 
    "type": "column",
    "table": "sales_transactions",
    "name": "total_amount",
    "description": "Column 'total_amount' in the 'sales_transactions' table represents the total price for this specific product line item within the transaction, calculated as 'quantity' multiplied by 'price_at_sale'."
  }
]
# --- End of schema_descriptions list ---

def upload_schema_to_gcs(bucket_name, destination_blob_name, schema_data):
    """Generates JSON from schema data and uploads it to GCS."""

    print("Generating JSON data...")
    # Convert the list of dictionaries to a JSON string with pretty printing
    json_output = json.dumps(schema_data, indent=2)
    print("JSON data generated.")

    try:
        # Instantiate the GCS client
        # Assumes Application Default Credentials (ADC) are set up
        storage_client = storage.Client()

        # Get the target bucket
        bucket = storage_client.bucket(bucket_name)

        # Get the target blob (file)
        blob = bucket.blob(destination_blob_name)

        print(f"Uploading JSON to gs://{bucket_name}/schema/{destination_blob_name}...")

        # Upload the JSON string directly
        # Specify content type for proper handling (e.g., by GCS console)
        blob.upload_from_string(
            data=json_output,
            content_type='application/json'
        )

        print(f"Successfully uploaded schema to gs://{bucket_name}/schema/{destination_blob_name}")

    except Exception as e:
        print(f"Error uploading schema to GCS: {e}")
        # Consider more specific error handling based on expected exceptions
        # from google.api_core.exceptions import NotFound, Forbidden

# --- Main execution ---
if __name__ == "__main__":
    # Basic validation for placeholders
    if "[YOUR_BUCKET_NAME]" in BUCKET_NAME or "[PATH_IN_BUCKET" in DESTINATION_BLOB_NAME:
        print("Error: Please replace the placeholder values for BUCKET_NAME and DESTINATION_BLOB_NAME in the script.")
    else:
        upload_schema_to_gcs(BUCKET_NAME, DESTINATION_BLOB_NAME, schema_descriptions)