def get_db_connection(retries=5, delay=3):
    pass

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