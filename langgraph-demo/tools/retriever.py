# /nl2sql-agent/tools/retriever.py

import os
import json
from typing import List, Dict, Any
from google.cloud import aiplatform
from google.cloud import storage
from langchain_google_vertexai import VertexAIEmbeddings
import config # Import configuration from config.py

def load_schema_lookup_from_gcs(gcs_uri: str) -> Dict[str, str]:
    """
    Downloads schema descriptions JSON from GCS and transforms it into a lookup dictionary.
    The JSON is expected to be a list of objects, each containing an 'id' field
    (matching the ID in Vector Search) and a 'description' field.
    """
    print(f"--- Loading schema lookup from GCS: {gcs_uri} ---")
    final_lookup_dict: Dict[str, str] = {}
    try:
        if not config.GCP_PROJECT_ID:
            print("[ERROR] GCP_PROJECT_ID is not configured. Cannot initialize GCS client.")
            return final_lookup_dict

        storage_client = storage.Client(project=config.GCP_PROJECT_ID)

        if not gcs_uri or not gcs_uri.startswith("gs://"):
            print(f"[ERROR] Invalid GCS URI provided: '{gcs_uri}'. It must start with 'gs://'.")
            return final_lookup_dict

        try:
            bucket_name, blob_name = gcs_uri[5:].split("/", 1)
        except ValueError:
            print(f"[ERROR] Invalid GCS URI format: '{gcs_uri}'. Expected: gs://bucket-name/path/to/blob.json")
            return final_lookup_dict

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if not blob.exists(storage_client):
            print(f"[ERROR] GCS file not found at {gcs_uri}. Please check the path and bucket.")
            return final_lookup_dict

        print(f"Attempting to download {blob_name} from bucket {bucket_name}...")
        json_data_string = blob.download_as_text(encoding='utf-8')
        print("File downloaded successfully.")

        loaded_json_list = json.loads(json_data_string)

        if not isinstance(loaded_json_list, list):
            print(f"[ERROR] Expected a JSON array (list) from GCS, but got type: {type(loaded_json_list)}. Check the JSON file structure.")
            return final_lookup_dict

        print(f"Processing {len(loaded_json_list)} items from JSON list to build lookup dictionary...")
        for item in loaded_json_list:
            if not isinstance(item, dict):
                print(f"[WARNING] Skipping non-dictionary item in JSON list: {str(item)[:100]}")
                continue

            doc_id = item.get('id') # <<< --- THIS IS THE CRITICAL LINE ---
            item_description = item.get('description')

            if not doc_id:
                print(f"[WARNING] Skipping item due to missing 'id' field: {str(item)[:150]}")
                continue
            if not item_description:
                print(f"[WARNING] Skipping item with ID '{doc_id}' due to missing 'description' field.")
                continue

            if doc_id in final_lookup_dict:
                print(f"[WARNING] Duplicate ID found in JSON: '{doc_id}'. Overwriting previous description. Ensure IDs are unique.")
            final_lookup_dict[doc_id] = item_description

        if not final_lookup_dict and loaded_json_list:
             print("[ERROR] Lookup dictionary is empty after processing, though the JSON list was not. Check for 'id' and 'description' fields in your JSON items.")
        else:
             print(f"Successfully processed JSON. Lookup dictionary built with {len(final_lookup_dict)} entries.")

    except json.JSONDecodeError:
        print(f"[ERROR] Failed to decode JSON from the file at {gcs_uri}. Ensure it's valid JSON.")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred while loading schema lookup from GCS: {e}")
        import traceback
        traceback.print_exc()

    if not final_lookup_dict:
        print("[CRITICAL WARNING] Schema lookup dictionary is empty after all attempts. Schema retrieval will fail.")
    return final_lookup_dict

# --- Load the Schema Lookup Dictionary at module import time ---
SCHEMA_DESCRIPTION_LOOKUP = load_schema_lookup_from_gcs(config.SCHEMA_LOOKUP_GCS_URI)


if not SCHEMA_DESCRIPTION_LOOKUP:
    print("[CRITICAL] Schema description lookup is empty. Schema retrieval will fail.")
    # Consider raising an ImportError or setting a flag indicating failure
else:
    print(f"[DEBUG INFO] tools/retriever.py: SCHEMA_DESCRIPTION_LOOKUP loaded with {len(SCHEMA_DESCRIPTION_LOOKUP)} entries.")
    loaded_keys = list(SCHEMA_DESCRIPTION_LOOKUP.keys()) # Get all keys
    print(f"[DEBUG INFO] First 5 keys in SCHEMA_DESCRIPTION_LOOKUP: {loaded_keys[:5]}") # Print a sample

# --- Initialize Vertex AI (can be done once at module level) ---
try:
    aiplatform.init(project=config.GCP_PROJECT_ID, location=config.GCP_REGION)
    print(f"Vertex AI SDK initialized in retriever.py for project '{config.GCP_PROJECT_ID}'.")
except Exception as e:
    print(f"Error initializing Vertex AI SDK in retriever.py: {e}")


# --- Schema Retrieval Function (as defined previously) ---
def retrieve_relevant_schema(query: str, index_endpoint_name: str, deployed_index_id: str, num_results: int = 5) -> str:
    """Embeds query and retrieves relevant schema descriptions from Vertex AI Vector Search."""
    print(f"\n--- Starting Schema Retrieval for query: '{query}' ---")

    if not SCHEMA_DESCRIPTION_LOOKUP:
         print("[ERROR] Cannot retrieve schema: Lookup dictionary is empty.")
         return "Failed to retrieve schema context: Lookup data missing." # Return error message

    # Ensure required config values are present
    if not all([index_endpoint_name, deployed_index_id, config.GCP_PROJECT_ID, config.GCP_REGION]):
         print("[ERROR] Missing required configuration for Vector Search.")
         return "Failed to retrieve schema context: Configuration missing."

    try:
        # Initialize embedding service inside function or reuse a global instance
        embeddings_service = VertexAIEmbeddings(
            model_name=config.EMBEDDING_MODEL_NAME,
            project=config.GCP_PROJECT_ID,
            # location=config.GCP_REGION # Location might not be needed here, depends on SDK version/defaults
        )
        # print(f"Using embedding model: {config.EMBEDDING_MODEL_NAME}") # Debug log

        query_embedding = embeddings_service.embed_query(query)
        # print(f"Query embedded into vector dim {len(query_embedding)}.") # Debug log

        index_endpoint = aiplatform.MatchingEngineIndexEndpoint(index_endpoint_name=index_endpoint_name)
        print(f"Connecting to endpoint: {index_endpoint_name}") # Debug log

        response = index_endpoint.find_neighbors(
            queries=[query_embedding],
            deployed_index_id=deployed_index_id,
            num_neighbors=num_results
        )
        print("Received response from Vector Search.") # Debug log

        #print('SCHEMA_DESCRIPTION_LOOKUP ---------',SCHEMA_DESCRIPTION_LOOKUP)
        #print('response-----',response)


        relevant_docs_text: List[str] = []
        if response and response[0]:
            neighbors = response[0]
            # print(f"Found {len(neighbors)} potential neighbors.") # Debug log
            for neighbor in neighbors:
                neighbor_id = neighbor.id
                description = SCHEMA_DESCRIPTION_LOOKUP.get(neighbor_id)
                if description:
                    relevant_docs_text.append(description)
                    # print(f"  Mapped ID '{neighbor_id}' to description.") # Debug log
                else:
                    print(f"[Warning] Could not find description for ID: '{neighbor_id}'. Check JSON and index IDs.")
        else:
            print("Vector Search returned no neighbors.")

        if not relevant_docs_text:
            print("No relevant schema descriptions were successfully retrieved.")
            return "No specific schema context found relevant to the question. Use general knowledge of tables: stores, products, sales_transactions."
        else:
            final_context = "\n\n---\n\n".join(relevant_docs_text)
            print(f"--- Successfully Retrieved Schema Context (length: {len(final_context)}) ---") # Avoid printing full context in production logs
            return final_context

    except Exception as e:
        print(f"[ERROR] An error occurred during schema retrieval: {e}")
        # Log the full error traceback for debugging
        import traceback
        traceback.print_exc()
        return "Failed to retrieve schema context due to an error."