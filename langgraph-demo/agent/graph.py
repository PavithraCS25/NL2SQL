# /nl2sql-agent/agent/graph.py

from langgraph.graph import StateGraph, END
from .state import AgentState # Import state definition
from .nodes import ( # Import node logic functions
    retrieve_schema_node,
    generate_sql_node,
    execute_sql_node,
    generate_response_node,
    handle_error_node,
    should_execute_sql,
    should_generate_response,
    sanitize_prompt_node,
    llm_classify_intent_few_shot,
    route_based_on_intent,
    sanitize_model_response_node
)


print("Defining agent graph...")

# Create a new state graph instance with the AgentState structure
workflow = StateGraph(AgentState)
workflow.add_node("sanitize_prompt", sanitize_prompt_node)
workflow.add_node("classify_intent",llm_classify_intent_few_shot)
# Add nodes to the graph. Each node corresponds to a function imported from nodes.py
workflow.add_node("retrieve_schema", retrieve_schema_node)
workflow.add_node("generate_sql", generate_sql_node)
workflow.add_node("execute_sql", execute_sql_node)
workflow.add_node("generate_response", generate_response_node)
workflow.add_node("handle_error", handle_error_node)
workflow.add_node("sanitize_response", sanitize_model_response_node)

# Define the entry point of the graph
workflow.set_entry_point("sanitize_prompt")
# Define the edges (transitions between nodes)
workflow.add_edge("sanitize_prompt", "classify_intent")

workflow.add_conditional_edges(
    "classify_intent",
    route_based_on_intent,
    {
        "generate_direct_response":"generate_response",
        "retrieve_schema":"retrieve_schema"
    }
)


# Define the edges (transitions between nodes)
workflow.add_edge("retrieve_schema", "generate_sql")

# Conditional edge after SQL generation: decide whether to execute or handle error
workflow.add_conditional_edges(
    "generate_sql",
    should_execute_sql, # Function to determine the next step
    {
        "execute_sql": "execute_sql",   # If SQL generated, go to execute_sql
        "handle_error": "handle_error" # If error or no SQL, go to handle_error
    }
)

# Conditional edge after SQL execution: decide whether to generate response or handle error
workflow.add_conditional_edges(
    "execute_sql",
    should_generate_response, # Function to determine the next step
    {
        "generate_response": "generate_response", # If results obtained, go to generate_response
        "handle_error": "handle_error"      # If execution failed, go to handle_error
    }
)

# Edges leading to the end of the graph
workflow.add_edge("generate_response", "sanitize_response") 
workflow.add_edge("sanitize_response", END) # Successful response generation ends the graph
workflow.add_edge("handle_error", END)      # Error handling also ends the graph

# Compile the graph into a runnable application
app = workflow.compile()

print("Agent graph compiled successfully.")

# The compiled 'app' object can now be imported and used in main.py