from .state import AgentState # Relative import
from tools.retriever import retrieve_relevant_schema, SCHEMA_DESCRIPTION_LOOKUP # Import function and potentially the loaded lookup
from tools.bigquery_executor import execute_bq_query
#from tools.llm_services import get_sql_generation_chain, get_response_generation_chain # Example: Get chains
import config
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from tools.llm_services import llm
from tools.bigquery_executor import bq_client
import json
import re
from tools.model_armor import ModelArmorPipeline
from google.cloud import modelarmor_v1

# Ensure lookup data is available (might need better handling if loading fails)
if not SCHEMA_DESCRIPTION_LOOKUP:
     print("[WARN] nodes.py: SCHEMA_DESCRIPTION_LOOKUP is empty.")
     # Raise error or handle appropriately

#Instantiate the Model Armor
pipeline = ModelArmorPipeline()
# --- Node Functions ---

def sanitize_prompt_node(state: AgentState) -> dict:
    """Node that sanitizes input and updates the question field in the state."""
    print("--- Sanitizing Prompt ---")
    original_question = state["question"]
    print(f"Original question received: '{original_question}'")
    
    # Initialize the dictionary for updates to the state
    # It's good practice to also store the original_question separately if needed later for comparison/logging
    update = {"original_question": original_question}
    
    try:
        # Perform sanitization
        response = pipeline.sanitize_prompt(prompt=original_question)
        
        # For robust debugging, let's check the type and value of filter_match_state
        match_state = response.sanitization_result.filter_match_state
        #print(f"Sanitization API response: filter_match_state is '{match_state}' (type: {type(match_state)})")

        if match_state == 2 and "sdp" in response.sanitization_result.filter_results: # This value indicates a filter match leading to sanitization
            sanitized_prompt = response.sanitization_result.filter_results['sdp'].sdp_filter_result.deidentify_result.data.text
            # Enhanced print to clearly show what sanitized_prompt contains, including its type.
            # This will help identify if it's an empty string, None, or actual content.
            #print(f"Sanitization occurred (match_state == 2). Sanitized prompt is: '{sanitized_prompt}' (type: {type(sanitized_prompt)})")
            
            update.update({
                "question": sanitized_prompt,  # Update 'question' to the sanitized version for the next node
                "is_safe": False,
                "error_message": "Input sanitized due to security concerns"
            })
        else:
            # Prompt is considered "clean" by the sanitizer, or the specific filter (match_state == 2) was not triggered.
            # In this case, the 'question' for the next node should be the original, unaltered question.
            print(f"Prompt deemed clean or no specific sanitization rule matched (match_state == {match_state}). Using original question: '{original_question}'")
            update.update({
                "question": original_question, # Explicitly set 'question' to the original for the next node
                "is_safe": True
                # No error_message is typically needed if it's deemed safe and not altered.
            })
            
        return update # Return the dictionary of changes to be merged into the AgentState
            
    except Exception as e:
        print(f"Error during sanitization process: {str(e)}")
        # In case of any error during sanitization, fallback to using the original question
        # and flag it as not safe due to the processing error.
        return {
            "question": original_question,  # Ensure 'question' for the next node is the original
            "original_question": original_question, # Keep a record of the original
            "is_safe": False,
            "error_message": f"Sanitization process error: {str(e)}"
        }
    
def sanitize_model_response_node(state: AgentState) -> dict:
    """Node that sanitizes output and updates the final response field in the state."""
    print("--- Sanitizing Model Response ---")
    original_response = state["final_response"]
    print(f"Original response received: '{original_response}'")
    sanitized_response=pipeline.sanitize_response(response=original_response)
    if sanitized_response.sanitization_result.filter_match_state == 2:
        return {
            "safe": False,
            "sanitized_response": sanitized_response.sanitized_model_response_data.text,
            "filter_results": sanitized_response.sanitization_result.filter_results
        }
    return {"safe": True, "original_response": original_response}

def llm_classify_intent_few_shot(state: AgentState) -> dict:
    """
    Simulates an LLM call for few-shot intent classification.
    Replace this with an actual LLM call in your application.
    """
    intent_categories = ["DATABASE_QUERY", "GENERAL_QUESTION"]
    
    # Construct the few-shot prompt
    prompt_template = f"""
    Classify the user's query into one of the following categories: {', '.join(intent_categories)}.
    Respond with only the category name.

    Here are some examples:
    User Query: "Show me sales figures for last quarter."
    Category: DATABASE_QUERY

    User Query: "What's your name?"
    Category: GENERAL_QUESTION

    User Query: "How many active users are there in Germany?"
    Category: DATABASE_QUERY

    User Query: "my email address is contact@example.com"
    Category: GENERAL_QUESTION

    User Query: "Just saying hi"
    Category: GENERAL_QUESTION
    ---
    Now classify the following:
    User Query: "{state["question"]}"
    Category:
    """
    
    #print(f"\n--- LLM Classification Prompt Sent (Simulated) ---\n{prompt_template}\n-------------------------------------------------")

    # ---- SIMULATED LLM RESPONSE ----
    # In a real application, you would invoke your LLM here:
    try:
        response = llm.invoke(prompt_template)
        classification_result = response.content.strip()
        if classification_result == "GENERAL_QUESTION":
            state["query_results"]=[]
        elif classification_result=="DATABASE_QUERY":
            state["query_results"]=None
        else:
            state["query_results"]=[]
        state["intent_type"]=classification_result
        return state
    
    #state["intent_type"]=classification_result
    except Exception as e:
        return {
            "question":"question",
            "intent_type": state["intent_type"],
            "error_message": f"Intent classification error: {str(e)}"
        }

def route_based_on_intent(state: AgentState) -> str:
    intent = state["intent_type"]
    print(f"Conditional Edge Check: Intent is '{intent}'")
    if intent == "GENERAL_QUESTION":
        return "generate_direct_response"  # New name for clarity
    elif intent == "DATABASE_QUERY":
        return "retrieve_schema"
    else:
        # Fallback: if intent is unclear, perhaps default to general or error
        print(f"Warning: Unknown intent '{intent}'. Defaulting to general response.")
        return "generate_direct_response"
    
def retrieve_schema_node(state: AgentState) -> dict:
    """Retrieves relevant schema context using RAG."""
    print("--- Retrieving Schema ---")
    #print('question to schema step:',state["question"])
    question = state["question"]
    try:
        # Replace with your actual endpoint name from GCP console
        vector_search_endpoint = config.VECTOR_SEARCH_INDEX_ENDPOINT_NAME
        deployed_index_id = config.VECTOR_SEARCH_DEPLOYED_INDEX_ID
        # Ensure retrieve_relevant_schema is correctly implemented (Step 3.5)
        schema_context = retrieve_relevant_schema(question, vector_search_endpoint, deployed_index_id, num_results=5)
        if not schema_context:
            print("Warning: No relevant schema found.")
            schema_context = "No specific schema context found. Please use general knowledge of the tables: stores, products, sales_transactions."
        return {"schema_context": schema_context}
    except Exception as e:
        print(f"Error retrieving schema: {e}")
        return {"error_message": f"Failed to retrieve schema information: {e}"}

def generate_sql_node(state: AgentState) -> dict:
    """Generates SQL query using the LLM."""
    print("--- Generating SQL ---")
    question = state["question"]
    schema_context = state["schema_context"]
    print('schema_context used:', schema_context)
    if not schema_context: # Handle case where schema retrieval failed silently
        return {"error_message": "Cannot generate SQL without schema context."}

    prompt = ChatPromptTemplate.from_messages([
        ("system", f"""You are an expert Google BigQuery SQL generator. Based ONLY on the provided schema context and the user's question, generate a valid BigQuery SQL query.

Key Guidelines:
1.  **Understand Location Mentions:**
    * The general hierarchy is: a country can contain multiple cities, and a city can contain multiple stores.
    * If the user's query mentions a specific location name (e.g., 'Jurong', 'Tampines', 'Alexandra'), **your primary interpretation should be that this refers to a `stores.store_name`.** Generate SQL to filter using `stores.store_name = 'LocationName'`.
    * Only consider interpreting the location as a `city` (e.g., `stores.city = 'LocationName'`) if the query explicitly states "city of [LocationName]", "in the city [LocationName]", or if a `store_name` interpretation is clearly impossible or nonsensical based on the question and the provided 'Schema Context'.
    * For filtering on such user-provided location names, use case-insensitive comparisons, e.g., `LOWER(stores.store_name) = LOWER('LocationName')`.

2.  **Joins:** When you need to combine information from multiple tables, carefully use FOREIGN KEY information provided in the 'Schema Context' to make correct JOINs.

3.  **Sales Terminology & Aggregations:**
    * 'Sales', 'revenue', or 'best-selling' (in terms of monetary value) typically refers to the `total_amount` column in the `sales_transactions` table. Aggregate using `SUM(total_amount)`.
    * 'Quantity sold' or 'best-selling' (in terms of units sold) refers to the `quantity` column in the `sales_transactions` table. Aggregate using `SUM(quantity)`.
    * If terms like "average sales" or "average quantity" are used, use the `AVG()` function on the respective columns.
    * If a term like "popular" or "frequent" is used without specifying by amount or quantity, and both `total_amount` and `quantity` are relevant and available, prioritize `SUM(quantity)`.

4.  **Filtering Text Values:**
    * When filtering text columns based on user-provided string values (e.g., product categories, names, etc.), apply case-insensitive comparisons by using `LOWER()` on both the column and the value, e.g., `LOWER(products.category) = LOWER('Furniture')`.
    * This does not apply if the schema context indicates that a column is inherently case-sensitive for matching or if the value contains SQL patterns intended for a `LIKE` clause (e.g., '%chair%').

5.  **Complex Queries (Subqueries, CTEs, Window Functions):**
    * You are capable of generating complex SQL. If the question requires rankings (like "top N"), period-over-period comparisons, or calculations within specific partitions, use Common Table Expressions (CTEs) and Window Functions (e.g., `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` , `SUM(...) OVER (...)`) as appropriate.

6.  **Table Naming:**
    * The relevant tables, if determined to be needed from the 'Schema Context', are `{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET_ID}.stores`, `{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET_ID}.products`, and `{config.GCP_PROJECT_ID}.{config.BIGQUERY_DATASET_ID}.sales_transactions`. ALWAYS use these fully qualified names.

7.  **Schema Adherence:**
    * ONLY use tables and columns mentioned in the 'Schema Context' section. Do not infer or use any tables/columns not listed there.
    * If crucial information (like a specific column for filtering or aggregation that you would normally expect) seems missing from the provided 'Schema Context' to answer the question accurately, output 'NO_QUERY'.

8.  **Output Format:**
    * Only output the SQL query.
    * Do not include any explanations, comments, or markdown formatting (like ```sql or ```).

9. **Invalid Queries & Ambiguity Handling:**
    * If a valid SQL query cannot be generated based on the input and the provided 'Schema Context' (e.g., required information is missing, or question is out of scope), output the exact string 'NO_QUERY'.
    * If the user's question is highly ambiguous even after applying these guidelines (e.g., a critical filter value is entirely unclear and cannot be reasonably inferred from the question or schema context), output 'NO_QUERY'.

Schema Context:
{schema_context}
"""),
        ("user", f"User Question: {question}")
    ])
    sql_generator_chain = prompt | llm | StrOutputParser()

    try:
        sql_query = sql_generator_chain.invoke({}) # Pass context implicitly via prompt
        print(f"Generated SQL attempt: {sql_query}")
        if "NO_QUERY" in sql_query or not sql_query.strip():
             return {"error_message": "Could not generate a SQL query for this question."}
        # Basic validation (can be improved)
        if not ("SELECT" in sql_query.upper() and "FROM" in sql_query.upper()):
             return {"error_message": f"Invalid SQL generated: {sql_query}"}
        return {"sql_query": sql_query.strip()}
    except Exception as e:
        print(f"Error generating SQL: {e}")
        return {"error_message": f"LLM failed to generate SQL: {e}"}
    
def extract_sql_from_markdown(llm_output_string: str) -> str:
    """
    Extracts a SQL query from a string that might contain a Markdown code block.
    Handles ```sql ... ```, ```SQL ... ```, ``` ... ``` etc.
    If multiple Markdown code blocks are present, it extracts the content of the first one.
    If no block is found, it assumes the input might be raw SQL and just strips it.
    """
    # Regex to find content within ```<optional_language_identifier> ... ```
    # - ```(?:[a-zA-Z0-9_]*)? : Matches the opening ``` followed by an optional language identifier (like 'sql', 'python', etc.).
    #                                The (?:...) is a non-capturing group. [a-zA-Z0-9_]* matches alphanumeric characters and underscores for the language.
    # - \s*\n? : Matches any whitespace characters (spaces, tabs) and then an optional newline.
    # - (.*?) : This is the capturing group (group 1). It captures any characters ('.')
    #           zero or more times ('*'), non-greedily ('?'). Non-greedy is important if there are multiple blocks.
    # - \n?\s*``` : Matches an optional newline, any whitespace, and the closing ```.
    # re.DOTALL allows '.' to match newline characters, so the SQL query can span multiple lines.
    # re.IGNORECASE makes the language identifier matching case-insensitive (e.g., ```SQL, ```sql).
    
    pattern = r"```(?:[a-zA-Z0-9_]*)?\s*\n?(.*?)\n?\s*```"
    
    match = re.search(pattern, llm_output_string, re.DOTALL | re.IGNORECASE)
    
    if match:
        # If a markdown block is found, extract its content (group 1)
        cleaned_sql = match.group(1).strip()
        return cleaned_sql
    else:
        # If no markdown block is found, assume the string is already the SQL query (or just needs stripping)
        return llm_output_string.strip()
    
def execute_sql_node(state: AgentState) -> dict:
    """Executes the SQL query against BigQuery."""
    print("--- Executing SQL ---")
    sql_query = state["sql_query"]
    if not sql_query:
        return {"error_message": "No SQL query to execute."}
    if not bq_client:
         return {"error_message": "BigQuery client is not available."}
    # Clean the SQL query
    cleaned_sql_query = extract_sql_from_markdown(sql_query)

    print(f"Original raw query: '{sql_query}'")
    print(f"Cleaned SQL query: '{cleaned_sql_query}'")

    print(f"Executing query: {cleaned_sql_query}")

    try:
        query_job = bq_client.query(cleaned_sql_query)
        results = query_job.result() # Waits for the job to complete
        # Convert results to a list of dictionaries for easier handling
        records = [dict(row) for row in results]
        print(f"Query returned {len(records)} records.")
        print('records:',records)
        # Limit results passed to LLM if too large (optional)
        max_results_for_llm = 50
        if len(records) > max_results_for_llm:
            print(f"Warning: Truncating results from {len(records)} to {max_results_for_llm} for LLM context.")
            # Consider summarizing large results instead of just truncating
            records = records[:max_results_for_llm]

        return {"query_results": records}
    except Exception as e:
        print(f"Error executing BigQuery query: {e}")
        # Provide specific BQ errors if possible
        return {"error_message": f"Failed to execute BigQuery query: {e}"}
    
def format_results(results):
    """
    Formats a list of dictionaries into a natural language string.
    Handles any key names and multiple fields per row.
    """
    if not results:
        return "No information found for your request."
    
    # Each row: join all key-value pairs into a string
    row_strings = []
    for row in results:
        # For each row, join key-value pairs (e.g., "product_name: POÄNG Armchair")
        fields = [str(value) for value in row.values()]
        row_strings.append(", ".join(fields))
    
    # For a single result, just return it
    if len(row_strings) == 1:
        return row_strings[0]
    # For multiple results, join with commas and 'and' for the last item
    return ", ".join(row_strings[:-1]) + ", and " + row_strings[-1]


def generate_response_node(state: AgentState) -> dict:
    """Generates the final natural language response."""
    print("--- Generating Response ---")
    question = state["question"]
    query_results = state["query_results"]

    if query_results is None: # Check for None explicitly, as empty list is valid
         return {"error_message": "No query results available to generate response."}

    # Handle empty results
    if not query_results:
        final_response = "I found no data matching your request."
        return {"final_response": final_response}

    # Prepare results for the prompt (e.g., format as JSON or a table string)
    #results_string = json.dumps(query_results, indent=2, default=str) # Use default=str for dates/times
    results_string = format_results(query_results)
    prompt = ChatPromptTemplate.from_messages([
        ("system", f"""You are a helpful assistant answering questions about {config.COMPANY} sales data.
        Based on the user's original question and the provided data (which is the result of a database query), formulate a clear and concise natural language answer.
        Do not mention the SQL query or the database. Just provide the answer to the question.

        Data:
        {results_string}
        """),
        ("user", f"Original Question: {question}")
    ])
    response_generator_chain = prompt | llm | StrOutputParser()

    try:
        final_response = response_generator_chain.invoke({})
        #print(f"Generated Response: {final_response}")
        return {"final_response": final_response}
    except Exception as e:
        print(f"Error generating response: {e}")
        return {"error_message": f"LLM failed to generate the final response: {e}"}

def handle_error_node(state: AgentState) -> dict:
    """Generates a user-facing error message."""
    print("--- Handling Error ---")
    error = state.get("error_message", "An unknown error occurred.")
    # You could add more sophisticated error routing here
    final_response = f"Sorry, I encountered an issue: {error}"
    return {"final_response": final_response}

# --- Conditional Logic ---

def should_execute_sql(state: AgentState) -> str:
    """Determines the next step after SQL generation."""
    print("--- Checking SQL Generation ---")
    if state.get("error_message"):
        print(f"Error flag set: {state['error_message']}. Routing to error handler.")
        return "handle_error" # Route to error handler if generation failed
    if state.get("sql_query"):
        print("SQL query generated. Proceeding to execution.")
        return "execute_sql" # Route to execution if SQL is present
    else:
        # This case shouldn't happen if generate_sql_node handles NO_QUERY correctly, but as a fallback:
        print("No SQL query generated and no error flag. Routing to error handler.")
        state["error_message"] = "Failed to produce a SQL query." # Set error message
        return "handle_error"

def should_generate_response(state: AgentState) -> str:
    """Determines the next step after SQL execution."""
    print("--- Checking SQL Execution ---")
    if state.get("error_message"):
        print(f"Error flag set during execution: {state['error_message']}. Routing to error handler.")
        return "handle_error" # Route to error handler if execution failed
    if state.get("query_results") is not None: # Check if results are present (even empty list is valid)
         print("SQL executed successfully. Proceeding to response generation.")
         return "generate_response"
    else:
        print("No query results found and no error flag. Routing to error handler.")
        state["error_message"] = "Query execution did not return results or failed silently." # Set error message
        return "handle_error"
