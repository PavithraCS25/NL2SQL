import sys
from agent.graph import app # Import the compiled graph application
import config # Ensure config is loaded (implicitly happens on import)
from utils.callbacks import CustomCallbackHandler # Optional

def main():
    print("--- NL2SQL Agent ---")
    # Optional: Initialize callbacks
    handler = CustomCallbackHandler()
    run_config = {"callbacks": [handler]}

    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
        print(f"Processing question: {question}")
        inputs = {"question": question}
        try:
            # Invoke the agent graph
            final_state = app.invoke(inputs, config=run_config) # Pass config if using callbacks

            # Print the final response or error
            response = final_state.get("final_response", "Agent finished without a final response.")
            error = final_state.get("error_message")
            if error and response == "Agent finished without a final response.": # If handle_error didn't set a final response
                print(f"\nAgent Error: {error}")
            else:
                 print(f"\nAgent Response:\n{response}")

        except Exception as e:
            print(f"\nAn unexpected error occurred during agent execution: {e}")

    else: # Interactive mode
         print("Enter your question (or type 'quit' to exit):")
         while True:
             question = input("> ")
             if question.lower() == 'quit':
                 break
             if not question:
                 continue

             inputs = {"question": question}
             try:
                 final_state = app.invoke(inputs, config=run_config)
                 response = final_state.get("final_response", "Agent finished without a final response.")
                 error = final_state.get("error_message")
                 if error and response == "Agent finished without a final response.":
                     print(f"Agent Error: {error}\n")
                 else:
                     print(f"Agent Response:\n{response}\n")
             except Exception as e:
                  print(f"\nAn unexpected error occurred: {e}\n")


if __name__ == "__main__":
    main()