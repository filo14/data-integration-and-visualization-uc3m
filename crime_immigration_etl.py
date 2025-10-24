import time, psycopg2, requests, pandas, country_converter
from psycopg2.extras import execute_values
import pycountry

# Connection parameters defined in ./database/compose.yaml
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydb"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"

# Years of which we take data
YEARS = range(2020, 2024, 1)

def map_countries():
    COUNTRIES_MAP = {}
    for country in pycountry.countries:
        normalised_name = country.name
        COUNTRIES_MAP[country.name.lower()] = normalised_name
        COUNTRIES_MAP[country.alpha_2.lower()] = normalised_name
        COUNTRIES_MAP[country.alpha_3.lower()] = normalised_name
        if hasattr(country, 'official_name'):
            COUNTRIES_MAP[country.official_name.lower()] = normalised_name
    return COUNTRIES_MAP

COUNTRIES_MAP = map_countries()

def normalise_country(country):
    if not country:
        return None
    return COUNTRIES_MAP.get(country.strip().lower(), country)

def map_countries():
    COUNTRIES_MAP = {}
    for country in pycountry.countries:
        normalised_name = country.name
        COUNTRIES_MAP[country.name.lower()] = normalised_name
        COUNTRIES_MAP[country.alpha_2.lower()] = normalised_name
        COUNTRIES_MAP[country.alpha_3.lower()] = normalised_name
        if hasattr(country, 'official_name'):
            COUNTRIES_MAP[country.official_name.lower()] = normalised_name
    return COUNTRIES_MAP

COUNTRIES_MAP = map_countries()

def normalise_country(country):
    if not country:
        return None
    return COUNTRIES_MAP.get(country.strip().lower(), country)

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
                    print(f"Successfully fetched population data for year {year}.")
                else:
                    print(f"No data found for year {year}.")

            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from World Bank API: {e}")
                all_population_data = []

        raw_population_df = pandas.DataFrame(all_population_data)

        return raw_population_df, aggregate_codes

    def extract_crime():
        pass

    def extract_immig():
        file_path = './data-sources/tps00176_linear_2_0.csv'

        df = pandas.read_csv(file_path)
        return df

    return extract_population(), extract_crime(), extract_immig()

def transform_data(raw_population_tuple, raw_crime, raw_immig):
    def transform_country_and_population(raw_population_tuple):
        population_df, aggregate_codes = raw_population_tuple

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

        # Extract country name from subfield and convert to the lowercase (where country exists)
        population_df = population_df[population_df['country'].notna()]
        population_df = population_df[population_df['country'].apply(lambda x: x.get('value') is not None)]

        # Create country_name from normalised country
        population_df['country_name'] = population_df['country'].apply(
            lambda x: normalise_country(x['value']).strip().lower()
        )

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

        print(f"Successfully transformed country and population data.")

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
        crime_df['convicts_per_100000'] = ((crime_df['convicts'] / crime_df['population']) * 100000).round(1)

        return crime_df[['country_iso3_id', 'year_id', 'convicts_per_100000']]

    def transform_immig(raw_immig, population_df):
        immig_df = raw_immig[['geo', 'TIME_PERIOD', 'OBS_VALUE']]

        cc = country_converter.CountryConverter()

        immig_df = immig_df[
            (immig_df['geo'].str.len() == 2) & 
            (immig_df['geo'].notna())
        ]
        immig_df['country_iso3_id'] = cc.convert(
            names=immig_df['geo'], 
            to='ISO3'
        )
        immig_df['year_id'] = immig_df['TIME_PERIOD'].astype(int)
        immig_df['immigration_total'] = pandas.to_numeric(
            immig_df['OBS_VALUE'].replace(':', 0),
            errors='coerce'
        )
        immig_df = immig_df.dropna(subset=['immigration_total'])

        # Merge and keep only immigrants with countries that exist in transformed population
        immig_df = immig_df.merge(
            population_df,
            on=['country_iso3_id', 'year_id'],
            how='inner',
            validate='many_to_one'
        )

        # Normalize immigrants per 100.000 inhabitants
        immig_df['immigration_per_100000'] = (immig_df['immigration_total'] / immig_df['population']) * 100000

        # Apply the rounding to two decimal places
        immig_df['immigration_per_100000'] = (
            immig_df['immigration_per_100000']
            .round(2) 
        )

        immig_df = immig_df[[ 'immigration_per_100000', 'country_iso3_id', 'year_id']]

        print(f"Successfully transformed immigration data.")

        return immig_df

    country_df, population_df = transform_country_and_population(raw_population_tuple)
    return country_df, population_df, None, transform_immig(raw_immig, population_df)

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
            print(f"Successfully inserted data into table {table_name}.")
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
        data_to_insert = [tuple(row) for row in t_immig.to_numpy()]
        conflict_rule = "ON CONFLICT (country_iso3_id, year_id) DO NOTHING"
        insert_data(conn, "immigration", "(immigration_per_100000, country_iso3_id, year_id)", data_to_insert, conflict_rule)

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

            print("--- Finished ETL ---")
            
        except Exception as e:
            print(f"Pipeline failed: {e}")
        finally:
            db_conn.close()