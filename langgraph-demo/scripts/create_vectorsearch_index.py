import time
from google.cloud import aiplatform
from google.cloud.aiplatform import MatchingEngineIndex, MatchingEngineIndexEndpoint   # Specific class for endpoint
from google.api_core import exceptions as google_exceptions # For more specific error handling
from google.cloud import storage
from google.cloud.aiplatform_v1.types.index import IndexDatapoint
import os # Optional
import json
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
# GCP Project and Location
GCP_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
GCP_REGION = os.getenv("GOOGLE_CLOUD_REGION") # e.g., "us-central1", "asia-southeast1"
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Index Configuration
INDEX_DISPLAY_NAME = "schema-rag-index" # IMPORTANT: Used for finding existing index
INDEX_DESCRIPTION = "Vector Search Index for Schema RAG created via SDK"
INDEX_DIMENSIONS = 768 # Must match embedding model
INDEX_DISTANCE_MEASURE = "COSINE_DISTANCE" # Or "DOT_PRODUCT_DISTANCE"
INDEX_METADATA_GCS_URI = f"gs://{BUCKET_NAME}/index_metadata/index_metadata.json" # Needs trailing '/'
# Index Endpoint Configuration
ENDPOINT_DISPLAY_NAME = "schema-rag-endpoint" # IMPORTANT: Used for finding existing endpoint

# Deployment Configuration
DEPLOYED_INDEX_ID = "deployed_schema_rag" # IMPORTANT: Used for finding existing deployment
DEPLOYMENT_DISPLAY_NAME = "Schema RAG v1 SDK Deployment"
DEPLOYMENT_MACHINE_TYPE = "e2-standard-2"
DEPLOYMENT_MIN_REPLICAS = 1
DEPLOYMENT_MAX_REPLICAS = 1

# Data Upsert Configuration
EMBEDDINGS_GCS_URI = f"gs://{BUCKET_NAME}/embeddings/schema_embeddings.jsonl" # Path to embeddings JSONL



# --- End Configuration ---


def load_embeddings_from_gcs(gcs_uri: str) -> list[IndexDatapoint]:
    """Loads embeddings from GCS URI and converts to IndexDatapoint list.
    
    Args:
        gcs_uri: Full GCS path to JSONL embeddings file (gs://bucket/path/file.jsonl)
    """
    # Parse GCS URI into bucket and blob path
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}. Must start with 'gs://'")
    
    # Remove 'gs://' and split into bucket/path components
    path_parts = gcs_uri[5:].split("/", 1)
    bucket_name = path_parts[0]
    
    if len(path_parts) > 1:
        blob_path = path_parts[1]
    else:
        raise ValueError(f"No blob path specified in GCS URI: {gcs_uri}")

    # Load data from GCS
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    data = blob.download_as_text()
    datapoints = []
    
    for line in data.splitlines():
        item = json.loads(line)
        datapoints.append(
            IndexDatapoint(
                datapoint_id=item["id"],
                feature_vector=item["embedding"]
            )
        )
    
    return datapoints

def setup_vector_search_idempotent():
    """
    Creates/Finds Vector Search Index & Endpoint, deploys index if not already deployed,
    and initiates data upsert. Handles existing resources based on display names/IDs.
    """

    print(f"Initializing Vertex AI for project {GCP_PROJECT_ID} in {GCP_REGION}...")
    aiplatform.init(project=GCP_PROJECT_ID, location=GCP_REGION)

    index = None
    endpoint = None
    index_resource_name = None
    endpoint_resource_name = None

    # --- 1. Find or Create Vector Search Index ---
    try:
        print(f"Checking for existing Index with display name: {INDEX_DISPLAY_NAME}...")
        indexes = MatchingEngineIndex.list(filter=f'display_name="{INDEX_DISPLAY_NAME}"', location=GCP_REGION)
        if indexes:
            index = indexes[0] # Use the first found index
            index_resource_name = index.resource_name
            print(f"Found existing Index: {index_resource_name}")
            if len(indexes) > 1:
                 print(f"Warning: Found {len(indexes)} indexes with the same display name. Using the first one.")
        else:
            print(f"No existing index found with display name {INDEX_DISPLAY_NAME}. Creating new index...")
            # Using TreeAh index (common for ANN)
            index = MatchingEngineIndex.create_tree_ah_index(
                display_name=INDEX_DISPLAY_NAME,
                description=INDEX_DESCRIPTION,
                dimensions=INDEX_DIMENSIONS,
                approximate_neighbors_count=15,
                distance_measure_type=INDEX_DISTANCE_MEASURE,
                index_update_method="STREAM_UPDATE",
                contents_delta_uri=INDEX_METADATA_GCS_URI,
                shard_size="SHARD_SIZE_SMALL"
            )
            index_resource_name = index.resource_name
            print(f"Index creation initiated. Index resource name: {index_resource_name}")
            print("Waiting for index creation to complete (this can take several minutes)...")
            # Wait for index creation LRO to complete - essential before deployment
            index.wait() # This method blocks until the LRO finishes
            print(f"Index {index_resource_name} created successfully.")

    except Exception as e:
        print(f"Error finding or creating Index: {e}")
        return # Stop execution if index cannot be found or created

    # --- 2. Find or Create Index Endpoint ---
    try:
        print(f"Checking for existing Index Endpoint with display name: {ENDPOINT_DISPLAY_NAME}...")
        endpoints = MatchingEngineIndexEndpoint.list(filter=f'display_name="{ENDPOINT_DISPLAY_NAME}"', location=GCP_REGION)
        if endpoints:
            endpoint = endpoints[0] # Use the first found endpoint
            endpoint_resource_name = endpoint.resource_name
            print(f"Found existing Index Endpoint: {endpoint_resource_name}")
            if len(endpoints) > 1:
                 print(f"Warning: Found {len(endpoints)} endpoints with the same display name. Using the first one.")
        else:
            print(f"No existing endpoint found with display name {ENDPOINT_DISPLAY_NAME}. Creating new endpoint...")
            endpoint = MatchingEngineIndexEndpoint.create(
                display_name=ENDPOINT_DISPLAY_NAME,
                public_endpoint_enabled=True
            )
            endpoint_resource_name = endpoint.resource_name
            print(f"Endpoint creation initiated. Endpoint resource name: {endpoint_resource_name}")
            print("Waiting for endpoint creation to complete...")
            endpoint.wait() # Block until endpoint is ready
            print(f"Endpoint {endpoint_resource_name} created successfully.")

    except Exception as e:
        print(f"Error finding or creating Index Endpoint: {e}")
        return # Stop execution

    # --- 3. Check Deployment Status and Deploy Index if Necessary ---
    try:
        # Ensure we have valid index and endpoint objects/resource names
        if not index_resource_name or not endpoint_resource_name:
             print("Error: Index or Endpoint resource name not available. Cannot deploy.")
             return

        # Get the endpoint object (needed to check deployed_indexes)
        # If we found an existing endpoint, 'endpoint' holds the object. If we created it, it also holds it.
        # If the script was interrupted between creation and here, we might need to get it again:
        if not endpoint: # If endpoint object wasn't retained (e.g., error handling path)
             endpoint = aiplatform.MatchingEngineIndexEndpoint(endpoint_name=endpoint_resource_name)
             print(f"Re-fetched endpoint object for {endpoint_resource_name}")

        # Check if the index is already deployed with the target DEPLOYED_INDEX_ID
        is_already_deployed = False
        if endpoint.deployed_indexes: # Check if the list is not empty
            for deployed_index in endpoint.deployed_indexes:
                if deployed_index.id == DEPLOYED_INDEX_ID:
                    is_already_deployed = True
                    print(f"Index with deployed ID '{DEPLOYED_INDEX_ID}' already exists on endpoint {endpoint_resource_name}.")
                    # Optional: Check if the deployed index resource matches the one we intend
                    if deployed_index.index != index_resource_name.split('/')[-1]: # Compare index ID part
                        print(f"Warning: Existing deployment '{DEPLOYED_INDEX_ID}' points to a different index ({deployed_index.index}) than intended ({index_resource_name}).")
                    break # Found the deployment ID, no need to check further

        if not is_already_deployed:
            print(f"Deploying Index {index_resource_name} to Endpoint {endpoint_resource_name} with deployed ID '{DEPLOYED_INDEX_ID}'...")
            # Deploy the index (using index resource name now, as Index object might not have full state after .list())
            # Re-fetch the index object just to be safe, using its resource name
            index_to_deploy = aiplatform.MatchingEngineIndex(index_name=index_resource_name)
            endpoint.deploy_index(
                index=index_to_deploy,
                deployed_index_id=DEPLOYED_INDEX_ID,
                display_name=DEPLOYMENT_DISPLAY_NAME,
                machine_type=DEPLOYMENT_MACHINE_TYPE,
                min_replica_count=DEPLOYMENT_MIN_REPLICAS,
                max_replica_count=DEPLOYMENT_MAX_REPLICAS,
            )
            print(f"Deployment initiated for deployed index ID: {DEPLOYED_INDEX_ID}.")
            print("!!! Deployment takes 20-60 minutes. Monitor progress in the Cloud Console. !!!")
            print("!!! Upsert datapoints only after deployment is complete. !!!")
        else:
             print("Skipping deployment step.")


    except Exception as e:
        print(f"Error during deployment check or initiation: {e}")
        # If deployment fails, manual intervention (e.g., undeploying via Console) might be needed.
        return # Stop if deployment check/initiation fails

    # --- 4. Upsert Datapoints ---
    # Note: This step should ideally run only *after* confirming deployment is 100% complete.
    # This script initiates it regardless, relying on the user to ensure deployment readiness.
    print("\n--- Initiating Datapoint Upsert ---")
    print(f"NOTE: Ensure the index ({index_resource_name}) is fully deployed with ID '{DEPLOYED_INDEX_ID}' before expecting upsert results.")

    try:
        # Use the index resource name which we reliably have
        the_index_for_upsert = aiplatform.MatchingEngineIndex(index_name=index_resource_name)
        # Load embeddings from GCS

        datapoints = load_embeddings_from_gcs(EMBEDDINGS_GCS_URI)

        print('datapoints -------',datapoints)

        print(f"Upserting datapoints from {EMBEDDINGS_GCS_URI} to index {the_index_for_upsert.resource_name}...")
        the_index_for_upsert.upsert_datapoints(datapoints=datapoints) # This is async

        print("Datapoint upsert process initiated. Monitor index datapoint count in the Cloud Console.")
        # Actual indexing takes time after this call returns.
    except Exception as e:
        print(f"Error upserting datapoints: {e}")


# --- Main execution ---
if __name__ == "__main__":
    # Basic validation for placeholders
    placeholders = {
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "GCP_REGION": GCP_REGION,
        "INDEX_METADATA_GCS_URI": INDEX_METADATA_GCS_URI,
        "EMBEDDINGS_GCS_URI": EMBEDDINGS_GCS_URI
    }
    # Simple check for common placeholder patterns
    if any("[YOUR_" in value or "[PATH_" in value and ('GCS_URI' in key) for key, value in placeholders.items()):
         # or value.endswith('/') == False
         print("Error: Please replace the placeholder values (e.g., [YOUR_GCP_PROJECT_ID], [YOUR_BUCKET_NAME], [PATH_...]) in the script's Configuration section.")
         print("Ensure GCS URIs (INDEX_METADATA_GCS_URI, EMBEDDINGS_GCS_URI) start with 'gs://' and the metadata URI ends with '/'.")
    else:
        setup_vector_search_idempotent()