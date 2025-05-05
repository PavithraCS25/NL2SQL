# /nl2sql-agent/tools/bigquery_executor.py

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError
import pandas as pd
from typing import List, Dict, Any, Optional
import config # Import configuration

# Initialize BigQuery client globally (or manage lifespan appropriately)
try:
    # Use project ID explicitly from config for clarity
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    print(f"BigQuery client initialized for project '{config.GCP_PROJECT_ID}'.")
except Exception as e:
    print(f"[CRITICAL] Failed to initialize BigQuery client: {e}. SQL execution will fail.")
    bq_client = None # Ensure bq_client is None if initialization fails

def execute_bq_query(sql_query: str) -> Optional[List[Dict[str, Any]]]:
    """
    Executes a SQL query against Google BigQuery and returns results.

    Args:
        sql_query: The SQL query string to execute.

    Returns:
        A list of dictionaries representing the query results,
        or None if the query fails or the client is unavailable.
    """
    print(f"--- Executing BigQuery Query ---") # Avoid logging the full query in production
    if not bq_client:
        print("[ERROR] BigQuery client is not available.")
        return None # Return None to indicate failure

    if not sql_query or not isinstance(sql_query, str):
        print("[ERROR] Invalid SQL query provided.")
        return None

    # Basic safety check (can be expanded significantly)
    disallowed_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "GRANT", "TRUNCATE", "ALTER"]
    if any(keyword in sql_query.upper() for keyword in disallowed_keywords):
        print(f"[ERROR] Query contains disallowed keywords: {sql_query}")
        return None # Reject potentially harmful queries

    try:
        print(f"Running query against dataset: {config.BIGQUERY_DATASET_ID} (inferred project: {config.GCP_PROJECT_ID})")
        # Note: Table names in the query should ideally be fully qualified
        # e.g., `your-project-id.your-dataset-id.table_name`
        # The LLM should be prompted to generate fully qualified names if possible.
        query_job = bq_client.query(sql_query)

        # Wait for the job to complete and fetch results
        print("Waiting for query job to complete...")
        results = query_job.result()
        print("Query job finished.")

        # Convert results to a list of dictionaries using Pandas for robust type handling
        df = results.to_dataframe(create_bqstorage_client=True) # Use BQ Storage API for speed
        records = df.to_dict('records')

        print(f"Query executed successfully, returned {len(records)} records.")
        return records

    except GoogleAPICallError as api_error:
        # Catch specific BQ API errors
        print(f"[ERROR] BigQuery API Error executing query: {api_error}")
        # Log query for debugging if needed, but be careful in production
        # print(f"Failed Query: {sql_query}")
        return None # Indicate failure
    except Exception as e:
        # Catch any other unexpected errors
        print(f"[ERROR] Unexpected error executing BigQuery query: {e}")
        # print(f"Failed Query: {sql_query}") # Log query for debugging
        import traceback
        traceback.print_exc()
        return None # Indicate failure

# Example of how to ensure fully qualified table names (could be a helper function)
def ensure_fully_qualified(sql: str, project: str, dataset: str) -> str:
#      # This is complex - requires SQL parsing. Simpler approach is to prompt LLM correctly.
#      # Basic example (very naive):
    sql = sql.replace(" FROM stores", f" FROM `{project}.{dataset}.stores`")
    sql = sql.replace(" JOIN products", f" JOIN `{project}.{dataset}.products`")
#      # ... needs proper parsing for robustness
    return sql