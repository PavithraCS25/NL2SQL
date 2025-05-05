# COMPANY Sales NL2SQL Agent

## 1. Overview

This project implements an advanced Natural Language to SQL (NL2SQL) agent specifically designed to query and provide insights from COMPANY's sales data. Users can ask complex questions in plain English (e.g., "What were the top 3 best-selling products by quantity sold in the Jurong store?", "Hello, how are you?"), and the agent will intelligently process these requests to deliver accurate, natural language answers.

The system architecture leverages cutting-edge AI models for language understanding, intent classification, and SQL generation. A robust graph-based framework (LangGraph) orchestrates the multi-step workflow, including a Retrieval Augmented Generation (RAG) engine for efficient schema utilization. **Crucially, user inputs and model outputs are sanitized using Google Cloud Model Armor, enhancing safety and adherence to responsible AI principles.**

## 2. Core Features

* **Natural Language Querying:** Accepts user questions in everyday English.
* **Secure Input Sanitization with Model Armor:** Pre-processes raw user input for consistency and cleaner handling, **including content filtering and security checks via Google Cloud Model Armor based on pre-configured templates.**
* **Intent Classification:** Employs an LLM (Gemini Flash series) with few-shot prompting to accurately determine user intent (e.g., data query, general question, greeting).
* **Conditional Routing:** Dynamically routes workflow based on classified intent, enabling different processing paths for SQL-based queries versus direct LLM responses.
* **Direct Response Capability:** Can handle non-database related questions or simple interactions directly via the LLM, bypassing the SQL pipeline.
* **Dynamic & Context-Aware SQL Generation:** Creates optimized BigQuery SQL queries tailored to the user's question, the relevant database schema, and contextual information like the current date (e.g., May 5, 2025).
* **Schema RAG Engine:** Utilizes Vertex AI Vector Search with schema descriptions (loaded from GCS) to retrieve only the most pertinent schema details for each query, enhancing SQL generation accuracy.
* **BigQuery Integration:** Executes generated SQL queries directly against the COMPANY sales data warehouse hosted on Google BigQuery.
* **Natural Language Responses:** Synthesizes query results (or direct LLM knowledge) and the original question into clear, concise, and user-friendly answers.
* **Secure Output Sanitization with Model Armor:** Post-processes the LLM's final natural language response for formatting, to remove any undesirable artifacts, **and to apply content filtering via Google Cloud Model Armor based on pre-configured templates.**
* **Advanced Contextual Understanding (for SQL path):**
    * Interprets ambiguous location mentions using predefined heuristics.
    * Differentiates sales-specific terminology.
    * Parses relative date expressions.
    * Applies case-insensitive filtering for text.
    * Capable of generating complex SQL constructs (CTEs, window functions).
* **Robust Workflow Orchestration:** Uses LangGraph to define and manage the agent's operational flow, including conditional logic and error handling pathways.
* **Detailed Operational Logging:** Integrates custom LangChain/LangGraph callbacks for comprehensive logging and tracing of agent activities.
* **Enhanced Security & Responsible AI:** Leverages Google Cloud Model Armor for proactive filtering of user prompts and model-generated responses, mitigating risks associated with harmful content or policy violations.

## 3. Key Technologies

* **Orchestration Framework:** LangGraph
* **Large Language Model (LLM):** Google Gemini Flash series (e.g., Gemini 1.5 Flash or latest available)
* **Database:** Google BigQuery
* **Vector Search (for Schema RAG):** Google Vertex AI Vector Search
* **Schema Description Storage:** Google Cloud Storage (GCS) for JSON-based schema details.
* **Security & Sanitization:** Google Cloud Model Armor
* **Programming Language:** Python
* **Core Libraries:** LangChain, Google Cloud Python SDKs.

## 4. Agent Workflow (Updated)

1.  **User Input:** The agent receives a natural language question.
2.  **Input Sanitization (`sanitize_prompt_node`):** The raw user input is processed by `sanitize_prompt_node`. **This node interacts with Google Cloud Model Armor to filter the input based on pre-configured security policies/templates.**
3.  **Intent Classification (`llm_classify_intent_few_shot`):** The sanitized question is passed to the Gemini LLM (using few-shot prompting) to determine if the query requires database interaction or can be answered directly.
4.  **Conditional Routing (`route_based_on_intent`):**
    * **Path A: Direct Response (Non-SQL)**
        * If the intent is classified as not requiring database lookup, the flow proceeds directly to generate a response using the LLM's general capabilities.
        * This path calls `generate_response_node`.
    * **Path B: Data Retrieval via SQL**
        * **Schema Retrieval (`retrieve_schema_node`):** The user's question is embedded, and Vertex AI Vector Search is queried to find relevant schema descriptions.
        * **SQL Generation (`generate_sql_node`):** The question, schema context, and current date are used by the Gemini LLM to generate a BigQuery SQL query. "NO_QUERY" is outputted if a query cannot be formed.
        * **SQL Cleaning:** Markdown or other extraneous formatting is stripped from the generated SQL.
        * **SQL Execution (`execute_sql_node` - Conditional):** If valid SQL was generated, it's executed against BigQuery.
        * **Response Generation (`generate_response_node`):** The original question and data retrieved from BigQuery are passed to the Gemini LLM to synthesize a natural language answer.
5.  **Output Sanitization (`sanitize_model_response_node`):** The LLM's final natural language response (from Path A or Path B) is processed by `sanitize_model_response_node`. **This node interacts with Google Cloud Model Armor for final content filtering based on pre-configured security policies/templates.**
6.  **Output:** The sanitized, natural language answer is presented to the user.
    * Error handling is managed by a dedicated `handle_error_node` and conditional logic within the LangGraph workflow.

## 5. Project Structure

A modular structure is adopted for maintainability:
```bash
.
├── agent
│   ├── __init__.py
│   ├── graph.py
│   ├── nodes.py
│   └── state.py
├── config.py
├── main.py
├── README.md
├── requirements.txt
├── schema_descriptions.json
├── scripts
│   ├── __init__.py
│   ├── create_vectorsearch_index.py
│   ├── data_generation.py
│   ├── generate_schema_embeddings.py
│   └── schema_generation.py
├── tools
│   ├── __init__.py
│   ├── bigquery_executor.py
│   ├── llm_services.py
│   ├── model_armor.py
│   └── retriever.py
└── utils
    ├── __init__.py
    └── callbacks.py
```

## 6. Setup & Prerequisites

1.  **Google Cloud Project:** An active GCP project with billing enabled.
2.  **APIs Enabled:** Ensure Vertex AI, BigQuery, Cloud Storage, **and any APIs required for Model Armor** are enabled for your project.
3.  **Authentication:** Configure application-default credentials or a service account with necessary IAM permissions (for Vertex AI, BigQuery, GCS, and Model Armor).
4.  **Python Environment:** Python 3.9+ recommended. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
5.  **Configuration (`.env` file):** Create and populate with GCP project details, GCS URI, Vector Search IDs, BigQuery dataset ID, LLM model names, **and any specific identifiers for Model Armor templates/policies if needed by your code.**
6.  **BigQuery Data Setup:** Load sales data into specified BigQuery tables.
7.  **Schema RAG Engine Setup:**
    * **Prepare Schema Descriptions:** Run `scripts/schema_generation.py` (or manually create) to produce the `schema_descriptions.json` file. This file must contain an `"id"` field for each schema item that exactly matches the ID to be used in Vector Search, and a corresponding `"description"`. Upload this JSON file to the GCS bucket and path specified in your `.env` (via `SCHEMA_LOOKUP_GCS_URI`).
    * **Populate Vector Search Index:** Run `scripts/generate_schema_embeddings.py`. This script should read your `schema_descriptions.json` (or its source), generate embeddings for the descriptions, and upload them to the Vector Search Index using the specified `"id"` for each document.
    * **Create Vector Search Infrastructure:** Run `scripts/create_vectorsearch_index.py` to load embeddings from GCS URI and converts to IndexDatapoint list and create/find Vector Search Index & Endpoint, deploy index if not already deployed,
    and initiates data upsert. Handles existing resources based on display names/IDs.
8.  **Model Armor Setup:**
    * **Define Security Policies/Templates:** Within Google Cloud Model Armor, you must pre-configure the necessary security policies or "templates" that define the sanitization rules (e.g., for harmful content categories, PII detection, prompt attack filtering). Your `sanitize_prompt_node` and `sanitize_model_response_node` will refer to these existing configurations.
9.  **Run the configuration file:** To setup the environment variables, run the `config.py`
    ```bash
    python3 config.py

## 7. Running the Agent

Once setup is complete, you can run the agent via the main script:
```bash
# For a single question:
python3 main.py "List the top 3 best-selling products with quantity sold at ’Jurong’"
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

Pavithra Sainath

