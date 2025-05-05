# /nl2sql-agent/tools/llm_services.py

import json
from typing import List, Dict, Any
from langchain_google_vertexai import ChatVertexAI
from typing import Optional, List, Dict, Any # Or whatever other types you need from 'typing'
import config # Import configuration

# --- Initialize LLM Client (globally) ---
try:
    llm = ChatVertexAI(
        model_name=config.GEMINI_MODEL_NAME,
        project=config.GCP_PROJECT_ID,
        location=config.GCP_REGION,
        temperature=0.1, # Lower temperature for more deterministic SQL generation
        # stream=False, # Set to True if streaming needed later
        # safety_settings=... # Configure safety settings if needed
    )
    print(f"LLM Client initialized with model: {config.GEMINI_MODEL_NAME}")
except Exception as e:
    print(f"[CRITICAL] Failed to initialize LLM Client: {e}. Agent will not function.")
    llm = None # Ensure llm is None if failed
