from typing import TypedDict, Optional, List, Dict, Any

class AgentState(TypedDict):
    question: str
    intent_type: Optional[str]
    schema_context: Optional[str]
    sql_query: Optional[str]
    query_results: Optional[List[Dict[str, Any]]]
    final_response: Optional[str]
    error_message: Optional[str]
    original_question: Optional[str]
    # Add other state variables if needed