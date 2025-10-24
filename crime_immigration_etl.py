import time, psycopg2, requests, pandas
from psycopg2.extras import execute_values

# Connection parameters defined in ./database/compose.yaml
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydb"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"

# Years of which we take data
YEARS = range(2020, 2025, 1)

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
    def extract_population():
        countries_response = requests.get('http://api.worldbank.org/v2/country?format=json&per_page=300')

        countries_response.raise_for_status()
        countries_data = countries_response.json()

        # all country ids whose region.value is "Aggregates"
        aggregate_codes = [c.get('id') for c in countries_data[1] if c.get('region', {}).get('value') == 'Aggregates']

        population_indicator = 'SP.POP.TOTL'
        all_population_data = []

        for year in YEARS:
            url = f'http://api.worldbank.org/v2/country/all/indicator/{population_indicator}?date={year}&format=json&per_page=2000'

            try:
                response = requests.get(url)
                response.raise_for_status() # Raise an HTTPError for bad responses
                data = response.json()

                # The first element of the response is metadata, the second is the data
                if data and len(data) > 1 and data[1] is not None:
                    # Append the year to each dictionary item before collecting
                    year_data = data[1]
                    for item in year_data:
                        # Inject the year into the raw dictionary to differentiate data by year
                        item['year_id'] = year 
                    
                    all_population_data.extend(year_data)
                    print(f"Successfully fetched raw data for year {year}.")
                else:
                    print(f"No data found for year {year}.")

            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from World Bank API: {e}")
                all_population_data = []

        return all_population_data, aggregate_codes

    def extract_crime():
        pass

    def extract_immig():
        pass

    return extract_population(), extract_crime(), extract_immig()

def transform_data(raw_population_tuple, raw_crime, raw_immig):
    def transform_country_and_population(raw_population_tuple):
        raw_population, aggregate_codes = raw_population_tuple

        # Convert to DataFrame
        population_df = pandas.DataFrame(raw_population)
        population_df = population_df[
            ~population_df['countryiso3code'].isin(aggregate_codes)
        ].copy()
    
        # Rename ISO3 field to match database
        population_df['country_iso3_id'] = population_df['countryiso3code']

        # Filter invalid ISO3
        population_df = population_df[
            (population_df['country_iso3_id'].str.len() > 0) & 
            (population_df['country_iso3_id'].notna())
        ]

        # Extract country name from subfield and convert to the lowercase
        population_df['country_name'] = population_df['country'].apply(lambda x: x['value'].strip().lower())

        # Convert population to numeric (normalise numbers)
        population_df['population'] = pandas.to_numeric(population_df['value'], errors='coerce')
        population_df = population_df[population_df['population'] > 0]

        # Drop rows where conversion to numeric failed or population is None
        population_df.dropna(subset=['population'], inplace=True)

        # Round population per integer number
        population_df['population'] = population_df['population'].round(0)
        population_df['population'] = population_df['population'].astype(int)

        # Check out the year (starting with 2020)
        population_df['year_id'] = population_df['year_id'].astype(int)
        population_df = population_df[population_df['year_id'] >= 2020]

        # Create database matching dataframes for country and population
        country_df = population_df[['country_iso3_id', 'country_name']].drop_duplicates(subset=['country_iso3_id']).copy()
        population_df = population_df[['population', 'country_iso3_id', 'year_id']].copy()

        return country_df, population_df
    
    def transform_crime(raw_crime, transformed_population_df):
        # Convert to DataFrame
        crime_df = pandas.DataFrame(raw_crime)

        # Rename ISO3 field to match database
        crime_df['country_iso3_id'] = crime_df['countryiso3code']

        # Convert crime to numeric
        crime_df['convicts'] = pandas.to_numeric(crime_df['value'], errors='coerce')
        crime_df = crime_df[crime_df['convicts'] >= 0]
        crime_df = crime_df[crime_df['convicts'].notna()]

        # Drop rows where conversion to numeric failed or crime is None
        crime_df.dropna(subset=['convicts'], inplace=True)

        # Merge and keep only crimes with countries that exist in transformed population
        crime_df = crime_df.merge(
            transformed_population_df,
            on=['country_iso3_id', 'year_id'],
            how='inner',
            validate='many_to_one'
        )

        # Normalize crime per 100.000 inhabitants
        crime_df['convicts_per_100000'] = (crime_df['convicts'] / crime_df['population']) * 100000

        return crime_df[['country_iso3_id', 'year_id', 'convicts_per_100000']]

    def transform_immig(raw_immig, transformed_population_df):
        # Convert to DataFrame
        immig_df = pandas.DataFrame(raw_immig)

        # Rename ISO3 field to match database
        immig_df['country_iso3_id'] = immig_df['countryiso3code']

        # Convert crime to numeric
        immig_df['immigrants'] = pandas.to_numeric(immig_df['value'], errors='coerce')
        immig_df = immig_df[immig_df['immigrants'] >= 0]
        immig_df = immig_df[immig_df['immigrants'].notna()]

        # Drop rows where conversion to numeric failed or crime is None
        immig_df.dropna(subset=['immigrants'], inplace=True)

        # Merge and keep only crimes with countries that exist in transformed population
        immig_df = immig_df.merge(
            transformed_population_df,
            on=['country_iso3_id', 'year_id'],
            how='inner',
            validate='many_to_one'
        )

        # Normalize crime per 100.000 inhabitants
        immig_df['immigration_per_100000'] = (immig_df['immigrants'] / immig_df['population']) * 100000

        return immig_df[['country_iso3_id', 'year_id', 'immigration_per_100000']]

    country_df, population_df = transform_country_and_population(raw_population_tuple)
    return country_df, population_df, transform_crime(raw_crime, population_df), transform_immig(raw_immig, population_df)

def load_data(conn, t_country, t_population, t_crime, t_immig):
    def insert_data(conn, table_name, columns, data_to_insert, conflict_rule=""):
        cur = conn.cursor()
        try:
            insert_query = f"INSERT INTO {table_name} {columns} VALUES %s {conflict_rule}"
            execute_values(
                cur,
                insert_query,
                data_to_insert,
                page_size=1000
            )
            conn.commit()
        except (Exception, psycopg2.Error) as error:
            print(f"Error during database operation: {error}")
            if conn:
                conn.rollback()
                
        finally:
            if conn:
                cur.close()

    def load_country(conn, t_country):
        data_to_insert = [tuple(row) for row in t_country.to_numpy()]
        conflict_rule = "ON CONFLICT (country_iso3_id) DO NOTHING"
        insert_data(conn, "country", "(country_iso3_id, country_name)", data_to_insert, conflict_rule)

    def load_population(conn, t_population):
        data_to_insert = [tuple(row) for row in t_population.to_numpy()]
        conflict_rule = "ON CONFLICT (country_iso3_id, year_id) DO NOTHING"
        insert_data(conn, "population","(population, country_iso3_id, year_id)", data_to_insert, conflict_rule)

    def load_crime(conn, t_crime):
        pass

    def load_immig(conn, t_immig):
        pass

    load_country(conn, t_country)
    load_population(conn, t_population)
    load_crime(conn, t_crime)
    load_immig(conn, t_immig)

    conn.close()

if __name__ == "__main__":
    db_conn = get_db_connection()
    
    if db_conn:
        try:
            # 1. E
            raw_population_tuple, raw_crime, raw_immig = extract_data()
            
            # 2. T
            t_country, t_population, t_crime, t_immig = transform_data(raw_population_tuple, raw_crime, raw_immig)
            
            # 3. L
            load_data(db_conn, t_country, t_population, t_crime, t_immig)
            
        except Exception as e:
            print(f"Pipeline failed: {e}")
        finally:
            db_conn.close()