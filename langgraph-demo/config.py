import os
from dotenv import load_dotenv

# Load variables from .env file if it exists
load_dotenv()

# --- GCP Configuration ---
GCP_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
GCP_REGION = os.environ.get("GOOGLE_CLOUD_REGION")
BIGQUERY_DATASET_ID = os.environ.get("BQ_DATASET_ID")
BIGQUERY_PROJECT_ID = GCP_PROJECT_ID # Often the same as the main project

# --- Vertex AI Vector Search ---
VECTOR_SEARCH_INDEX_ENDPOINT_NAME = os.environ.get("VECTOR_SEARCH_INDEX_ENDPOINT_NAME")
VECTOR_SEARCH_DEPLOYED_INDEX_ID = os.environ.get("VECTOR_SEARCH_DEPLOYED_INDEX_ID")
SCHEMA_LOOKUP_GCS_URI = os.environ.get("SCHEMA_LOOKUP_GCS_URI")
EMBEDDINGS_GCS_JSONL_PATH = os.environ.get("EMBEDDINGS_GCS_JSONL_PATH")
EMBEDDING_MODEL_NAME = os.environ.get("EMBEDDING_MODEL_NAME", "text-embedding-004") # Default if not set

# --- LLM Configuration ---
GEMINI_MODEL_NAME = os.environ.get("GEMINI_MODEL_NAME", "gemini-2.0-flash-001")
COMPANY = os.environ.get("COMPANY_NAME")


#Model Armor template id
MA_TEMPLATE_ID = os.environ.get("MA_TEMPLATE_ID")

# --- Basic Validation (Optional but Recommended) ---
required_vars = [
    GCP_PROJECT_ID, GCP_REGION, BIGQUERY_DATASET_ID,
    VECTOR_SEARCH_INDEX_ENDPOINT_NAME, VECTOR_SEARCH_DEPLOYED_INDEX_ID,
    SCHEMA_LOOKUP_GCS_URI,EMBEDDINGS_GCS_JSONL_PATH, MA_TEMPLATE_ID,COMPANY
]
if not all(required_vars):
    missing = [name for name, var in locals().items() if name.isupper() and var is None and name != 'BIGQUERY_PROJECT_ID'] # Simple check
    raise ValueError(f"Missing required environment variables: {missing}")

print("Configuration loaded successfully.")
# You might add more checks (e.g., validate GCS URI format)