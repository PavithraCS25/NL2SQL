import os
import vertexai
import vertexai.generative_models as genai
from google.cloud import modelarmor_v1
import config
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ModelArmorPipeline:
    def __init__(self,):
        self.project_id = config.GCP_PROJECT_ID
        self.location = config.GCP_REGION
        self.model_name = config.GEMINI_MODEL_NAME,
        
        # Initialize clients
        vertexai.init(project=self.project_id, location=self.location)
        self.genai_client = genai.GenerativeModel(self.model_name)
        self.model_armor_client = modelarmor_v1.ModelArmorClient(
            transport="rest",
            client_options={"api_endpoint": "modelarmor.us-central1.rep.googleapis.com"},
        )

    def sanitize_prompt(self, prompt: str, template_id: str = config.MA_TEMPLATE_ID) -> dict:
        """Sanitize user prompt using Model Armor"""
        try:
            prompt_data = modelarmor_v1.DataItem(text=prompt)
            request = modelarmor_v1.SanitizeUserPromptRequest(
                name=f"projects/{self.project_id}/locations/{self.location}/templates/{template_id}",
                user_prompt_data=prompt_data,
            )
            response = self.model_armor_client.sanitize_user_prompt(request=request)
            
            return response
            
        except Exception as e:
            raise RuntimeError(f"Model Armor prompt sanitization failed: {e}")

    def sanitize_response(self, response: str, template_id: str = config.MA_TEMPLATE_ID) -> dict:
        """Sanitize model response using Model Armor"""
        try:
            response_data = modelarmor_v1.DataItem(text=response)
            request = modelarmor_v1.SanitizeModelResponseRequest(
                name=f"projects/{self.project_id}/locations/{self.location}/templates/{template_id}",
                model_response_data=response_data,
            )
            sanitized_response = self.model_armor_client.sanitize_model_response(request=request)
            
            return sanitized_response
            
        except Exception as e:
            raise RuntimeError(f"Model Armor response sanitization failed: {e}")