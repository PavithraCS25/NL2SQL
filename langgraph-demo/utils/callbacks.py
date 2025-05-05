# /nl2sql-agent/utils/callbacks.py

from langchain_core.callbacks import BaseCallbackHandler
from typing import Any, Dict, List, Optional, Union
from langchain_core.messages import BaseMessage
import time # Example: to time operations

class CustomCallbackHandler(BaseCallbackHandler):
    """A custom callback handler for logging and timing agent steps."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.chain_start_time = None
        self.llm_start_time = None
        self.tool_start_time = None
        print("CustomCallbackHandler initialized.")

    def on_chain_start(
        self, serialized: Dict[str, Any], inputs: Dict[str, Any], **kwargs: Any
    ) -> Any:
        """Called when a chain (like a graph node execution) starts."""
        self.chain_start_time = time.time()
        chain_name = "Unknown/Unnamed Chain" # Default value

        if serialized: # <<< This is the crucial check!
            # Attempt to get a more descriptive name if available
            name_from_key = serialized.get('name')
            if name_from_key:
                chain_name = name_from_key
            else:
                # 'id' key often contains a list representing the object's path/class
                id_list = serialized.get('id')
                if id_list and isinstance(id_list, list) and len(id_list) > 0:
                    # The last element is usually the most specific class name
                    chain_name = id_list[-1]
                # If neither 'name' nor 'id' provides a good string,
                # it will remain "Unknown/Unnamed Chain"
        else:
            # Log that serialized was None, which is the cause of the original error
            print(f"\n>> Entering Chain: [Serialized object was None, check component definition]") # Helps identify the problematic step

        print(f"\n>> Entering Chain: {chain_name}")
        # Example: Log partial inputs (be careful with sensitive data)
        # print(f"   Inputs (partial): {{'question': inputs.get('question', '?')}}")

    def on_chain_end(self, outputs: Dict[str, Any], **kwargs: Any) -> Any:
        """Called when a chain ends."""
        duration = f"{(time.time() - self.chain_start_time):.2f}s" if self.chain_start_time else "N/A"
        print(f"<< Exiting Chain (Duration: {duration})")
        # Example: Log partial outputs
        # print(f"   Outputs (keys): {list(outputs.keys())}")
        self.chain_start_time = None # Reset timer

    def on_llm_start(
        self, serialized: Dict[str, Any], prompts: List[str], **kwargs: Any
    ) -> Any:
        """Called when an LLM call starts."""
        self.llm_start_time = time.time()
        model_name = serialized.get('kwargs', {}).get('model_name', 'Unknown LLM')
        print(f"  >> LLM Call Start ({model_name})")
        # Log prompts if needed for debugging (can be verbose)
        # print(f"     Prompt:\n{prompts[0][:500]}...") # Log first 500 chars

    def on_llm_end(self, response, **kwargs: Any) -> Any:
        """Called when an LLM call ends."""
        duration = f"{(time.time() - self.llm_start_time):.2f}s" if self.llm_start_time else "N/A"
        # Accessing token usage might require specific model/provider parsing
        # token_usage = response.llm_output.get('token_usage', {}) if hasattr(response, 'llm_output') else {}
        # print(f"  << LLM Call End (Duration: {duration}, Tokens: {token_usage})")
        print(f"  << LLM Call End (Duration: {duration})")
        # print(f"     Response (partial): {str(response.generations[0][0].text)[:200]}...") # Log partial response
        self.llm_start_time = None # Reset timer

    def on_llm_error(
        self, error: Union[Exception, KeyboardInterrupt], **kwargs: Any
    ) -> Any:
        """Called when an LLM call errors."""
        print(f"  [ERROR] LLM Error: {error}")
        self.llm_start_time = None # Reset timer
