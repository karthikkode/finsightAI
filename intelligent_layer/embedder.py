import ollama
import logging
from typing import List, Optional
from . import config

class EmbeddingGenerator:
    """
    Handles the generation of vector embeddings using a local Ollama model.
    """
    def __init__(self, model_name: str = config.EMBEDDING_MODEL):
        self.model_name = model_name
        logging.info(f"EmbeddingGenerator initialized with model: {self.model_name}")

    def generate_embedding(self, text: str) -> Optional[List[float]]:
        if not text:
            logging.warning("Attempted to generate embedding for empty text.")
            return None
        try:
            logging.debug(f"Generating embedding for text: '{text[:50]}...'")
            response = ollama.embeddings(model=self.model_name, prompt=text)
            return response.get('embedding')
        except Exception as e:
            logging.error(f"Failed to generate embedding with model {self.model_name}: {e}")
            return None
