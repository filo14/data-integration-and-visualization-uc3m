import time, psycopg2, requests, pandas

# Connection parameters defined in ./database/compose.yaml
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
    def extract_countries():
        countries_response = requests.get('http://api.worldbank.org/v2/country?format=json&per_page=300')

        countries_response.raise_for_status()
        countries_data = countries_response.json()

        # all country ids whose region.value is "Aggregates"
        aggregate_codes = [c.get('id') for c in countries_data[1] if c.get('region', {}).get('value') == 'Aggregates']

        population_indicator = 'SP.POP.TOTL'
        year = 2022
        url = f'http://api.worldbank.org/v2/country/all/indicator/{population_indicator}?date={year}&format=json&per_page=2000'

        try:
            response = requests.get(url)
            response.raise_for_status() # Raise an HTTPError for bad responses
            data = response.json()

            # The first element of the response is metadata, the second is the data
            if data and len(data) > 1:
                population_data = data[1]
            else:
                population_data = []
                print("No data found for the specified year and indicator.")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from World Bank API: {e}")
            population_data = []

        # Convert to DataFrame
        population_df = pandas.DataFrame(population_data)
        population_df = population_df[
            ~population_df['countryiso3code'].isin(aggregate_codes)
        ].copy()
        return population_df

    pass

def transform_data(population_df, crime_df, immigration_df):
    def transform_countries():
        # iso3 code for filtering
        population_df['ISO3_Code'] = population_df['countryiso3code']

        # extract country name and population value
        population_df['Country'] = population_df['country'].apply(lambda x: x['value'])
        population_df['Population'] = pandas.to_numeric(population_df['value'], errors='coerce')

        # drop rows where conversion to numeric failed or population is None
        population_df.dropna(subset=['Population'], inplace=True)

        population_df = population_df[['ISO3_Code', 'Country', 'Population']]

    pass

def load_data(conn):
    pass

if __name__ == "__main__":
    db_conn = get_db_connection()
    
    if db_conn:
        try:
            # 1. E
            raw_population, raw_crime, raw_immig = extract_data()
            
            # 2. T
            final_data = transform_data(raw_population, raw_crime, raw_immig)
            
            # 3. L
            load_data(db_conn, final_data)
            
        except Exception as e:
            print(f"Pipeline failed: {e}")
        finally:
            db_conn.close()