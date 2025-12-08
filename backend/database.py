import os
import psycopg2
from psycopg2.extras import RealDictCursor
import time

# Connection parameters with defaults from the ETL script
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "mydb")
DB_USER = os.getenv("DB_USER", "myuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mypassword")

def get_db_connection():
    """
    Establishes a connection to the database.
    Retries a few times if the connection fails.
    """
    conn = None
    retries = 3
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                cursor_factory=RealDictCursor
            )
            return conn
        except psycopg2.OperationalError as e:
            if attempt < retries - 1:
                time.sleep(1)
            else:
                raise e
    return None
