import time, psycopg2

DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydb"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"

def get_db_connection(retries=5, delay=3):
    print(f"Attempting to connect to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    
    conn = None
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            print("Successfully connected to the database!")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < retries - 1:
                print(f"Connection failed (Attempt {attempt + 1}/{retries}): {e}")
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"Failed to connect after {retries} attempts.")
                print("Ensure your Docker container is running and the port 5432 is exposed.")
                raise e

    return None

def extract_data():
    pass

def transform_data(crime_df, immigration_df):
    pass

def load_data(conn, final_df):
    pass

if __name__ == "__main__":
    db_conn = get_db_connection()
    
    if db_conn:
        try:
            # 1. E
            raw_crime, raw_immig = extract_data()
            
            # 2. T
            final_data = transform_data(raw_crime, raw_immig)
            
            # 3. L
            load_data(db_conn, final_data)
            
        except Exception as e:
            # Catch errors and clean up (e.g., rollback database changes)
            print(f"Pipeline failed: {e}")
        finally:
            db_conn.close()