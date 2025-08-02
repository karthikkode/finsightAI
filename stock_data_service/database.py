import psycopg2
from psycopg2 import OperationalError

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname="finsight_db",
            user="finsight_admin",
            password="1234",  # WARNING: Use environment variables in production
            host="localhost",
            port="5432"
        )
        print("✅ Database connection successful")
        return conn
    except OperationalError as e:
        print(f"❌ Could not connect to the database: {e}")
        return None
