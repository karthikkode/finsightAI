import os

# --- Database Configuration for the Main App and Scheduler ---
DB_NAME = os.getenv("DB_NAME", "finsight_db")
DB_USER = os.getenv("DB_USER", "finsight_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1234")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
