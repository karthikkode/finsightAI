import logging

# --- Logging Configuration ---
# This initializes logging for this specific service. When the updater
# script imports this config, the logging will be automatically set up.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
