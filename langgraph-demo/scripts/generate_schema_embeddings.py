import json
import time
from google.cloud import storage
from google.cloud import aiplatform
from langchain_google_vertexai import VertexAIEmbeddings
import os # Optional

from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
# GCP Project and Location
GCP_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
GCP_REGION = os.getenv("GOOGLE_CLOUD_REGION") # e.g., "us-central1", "asia-southeast1"

# GCS Bucket/Paths
# Input schema JSON location
SCHEMA_GCS_BUCKET = os.getenv("BUCKET_NAME")
SCHEMA_GCS_JSON_PATH = "schema/schema_descriptions.json" # Path from previous step

# Output embeddings JSONL location (Vector Search uses this)
EMBEDDINGS_GCS_BUCKET = os.getenv("BUCKET_NAME") # Can be the same bucket
EMBEDDINGS_GCS_JSONL_PATH = "embeddings/schema_embeddings.jsonl" # Choose output path/name

# Vertex AI Embedding Model
# Make sure the chosen model's dimensions match your Vector Search index dimensions (e.g., 768 for gecko)
EMBEDDING_MODEL_NAME = "text-embedding-004"
# --- End Configuration ---

def generate_and_upload_embeddings(project_id, region, schema_bucket, schema_path, embeddings_bucket, embeddings_path, model_name):
    """
    Loads schema descriptions, generates embeddings, formats as JSONL,
    and uploads the JSONL file to GCS for Vector Search indexing.
    """
    print("Starting embedding generation process...")

    # --- 1. Load Schema Descriptions from GCS ---
    try:
        print(f"Loading schema descriptions from gs://{schema_bucket}/{schema_path}...")
        storage_client = storage.Client(project=project_id)
        input_bucket = storage_client.bucket(schema_bucket)
        input_blob = input_bucket.blob(schema_path)
        schema_content = input_blob.download_as_string()
        schema_data = json.loads(schema_content)
        print('schema_data',schema_data)
        print(f"Loaded {len(schema_data)} schema descriptions.")
    except Exception as e:
        print(f"Error loading schema descriptions from GCS: {e}")
        return

    # --- 2. Initialize Vertex AI & Embeddings Service ---
    try:
        print(f"Initializing Vertex AI for project {project_id} in {region}...")
        aiplatform.init(project=project_id, location=region)
        print(f"Using embedding model: {model_name}")
        # Increase default timeout for potentially long embedding calls
        embeddings_service = VertexAIEmbeddings(
            model_name=model_name,
            request_parallelism=5 # Adjust based on quota/needs
        )
        print("Vertex AI Embeddings service initialized.")
    except Exception as e:
        print(f"Error initializing Vertex AI or Embeddings service: {e}")
        return

    # --- 3. Prepare Data for Embedding ---
    # Extract only the 'description' field which will be embedded
    description_texts = [item['description'] for item in schema_data]

    # Create unique IDs for each description - important for Vector Search
    # Making IDs somewhat informative can help debugging
    schema_ids = []
    for i, item in enumerate(schema_data):
        type_str = item.get('type', 'unknown')
        table_str = item.get('table', '') # Empty for table descriptions
        name_str = item.get('name', f'item_{i}')
        # Basic sanitization for ID (replace spaces, ensure valid chars if needed)
        unique_id = f"schema_{type_str}_{table_str}_{name_str}".replace('__','_').replace(' ','_').lower()
        # Add index to ensure uniqueness if names clash after sanitization
        unique_id = f"{unique_id}_{i}"
        schema_ids.append(unique_id)

    if not description_texts:
        print("No description texts found to embed.")
        return

    # --- 4. Generate Embeddings ---
    try:
        print(f"Generating embeddings for {len(description_texts)} descriptions...")
        start_time = time.time()
        # This might take some time depending on the number of descriptions
        vectors = embeddings_service.embed_documents(description_texts)
        end_time = time.time()
        print(f"Embeddings generated successfully in {end_time - start_time:.2f} seconds.")
        if len(vectors) != len(description_texts):
             print(f"Warning: Number of vectors ({len(vectors)}) does not match number of descriptions ({len(description_texts)}).")
             # Handle potential partial failures if necessary
             return # Or adjust logic
    except Exception as e:
        print(f"Error generating embeddings: {e}")
        return

    # --- 5. Format Data for Vector Search JSONL ---
    print("Formatting data into JSONL format...")
    jsonl_lines = []
    for i, vector in enumerate(vectors):
        if i < len(schema_ids): # Ensure we have a corresponding ID
            record = {
                "id": schema_ids[i],
                "embedding": vector
                # Note: Vector Search 'restricts' (metadata) are not added here,
                # but you have schema_ids mapped back to schema_data if needed later.
                # If adding restrictions, the JSON structure would change.
            }
            jsonl_lines.append(json.dumps(record))
        else:
            print(f"Warning: Skipping vector {i} due to missing corresponding ID.")

    jsonl_content = "\n".join(jsonl_lines)
    print(f"Generated {len(jsonl_lines)} lines for JSONL file.")

    # --- 6. Upload JSONL to GCS ---
    try:
        print(f"Uploading embeddings JSONL to gs://{embeddings_bucket}/{embeddings_path}...")
        output_bucket = storage_client.bucket(embeddings_bucket) # Use same client
        output_blob = output_bucket.blob(embeddings_path)

        # Upload the JSONL string content
        output_blob.upload_from_string(
            data=jsonl_content,
            content_type='application/json' # Often treated as plain text by GCS, but good practice
        )
        print(f"Successfully uploaded embeddings JSONL to gs://{embeddings_bucket}/{embeddings_path}")
        print("\nProcess Complete. The embeddings JSONL file is ready in GCS for index upsert.")

    except Exception as e:
        print(f"Error uploading embeddings JSONL to GCS: {e}")

# --- Main execution ---
if __name__ == "__main__":
    # Basic validation for placeholders
    placeholders = {
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "GCP_REGION": GCP_REGION,
        "SCHEMA_GCS_BUCKET": SCHEMA_GCS_BUCKET,
        "SCHEMA_GCS_JSON_PATH": SCHEMA_GCS_JSON_PATH,
        "EMBEDDINGS_GCS_BUCKET": EMBEDDINGS_GCS_BUCKET,
        "EMBEDDINGS_GCS_JSONL_PATH": EMBEDDINGS_GCS_JSONL_PATH
    }
    if any("[YOUR_" in value or "[PATH_IN_BUCKET" in value for value in placeholders.values()):
         print("Error: Please replace the placeholder values (e.g., [YOUR_GCP_PROJECT_ID], [YOUR_BUCKET_NAME], [PATH_IN_BUCKET/...]) in the script's Configuration section.")
    else:
        generate_and_upload_embeddings(
            GCP_PROJECT_ID,
            GCP_REGION,
            SCHEMA_GCS_BUCKET,
            SCHEMA_GCS_JSON_PATH,
            EMBEDDINGS_GCS_BUCKET,
            EMBEDDINGS_GCS_JSONL_PATH,
            EMBEDDING_MODEL_NAME
        )