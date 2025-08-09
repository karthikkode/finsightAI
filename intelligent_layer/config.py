import logging
import os

# --- Logging Configuration ---
# This initializes logging for this specific service.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Service Configuration ---
# The directory where the fetcher services have downloaded the raw documents.
# We read this from an environment variable for flexibility, with a sensible default.
SOURCE_DOCUMENTS_DIR = os.getenv("SOURCE_DOCUMENTS_DIR", "financial_reports")

# The name of the local Ollama model to use for generating embeddings.
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "mxbai-embed-large")
