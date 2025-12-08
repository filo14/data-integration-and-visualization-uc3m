# random queries to csv
import country_converter
import pandas
import pycountry
import requests
import plotly.graph_objects as go

YEARS = range(2018, 2023, 1)


def map_countries():
    COUNTRIES_MAP = {}
    for country in pycountry.countries:
        normalised_name = country.name
        COUNTRIES_MAP[country.name.lower()] = normalised_name
        COUNTRIES_MAP[country.alpha_2.lower()] = normalised_name
        COUNTRIES_MAP[country.alpha_3.lower()] = normalised_name
        if hasattr(country, "official_name"):
            COUNTRIES_MAP[country.official_name.lower()] = normalised_name
    return COUNTRIES_MAP


COUNTRIES_MAP = map_countries()


def normalise_country(country):
    if not country:
        return None
    return COUNTRIES_MAP.get(country.strip().lower(), country)


def extract_data():
    def extract_population():
        countries_response = requests.get(
            "http://api.worldbank.org/v2/country?format=json&per_page=300"
        )

        countries_response.raise_for_status()
        countries_data = countries_response.json()
        country_coord = pandas.DataFrame()
        list_c = []
        list_lon = []
        list_lat = []
        for c in countries_data[1]:
            if c.get('latitude') != '':
                list_c.append(c.get('name').lower())
                list_lon.append(c.get('longitude'))
                list_lat.append(c.get('latitude'))
        country_coord["country_name"] = list_c
        country_coord["lon"] = list_lon
        country_coord["lat"] = list_lat

        # all country ids whose region.value is "Aggregates"
        aggregate_codes = [
            c.get("id")
            for c in countries_data[1]
            if c.get("region", {}).get("value") == "Aggregates"
        ]

        population_indicator = "SP.POP.TOTL"
        all_population_data = []

        for year in YEARS:
            url = f"http://api.worldbank.org/v2/country/all/indicator/{population_indicator}?date={year}&format=json&per_page=2000"

            try:
                response = requests.get(url)
                response.raise_for_status()  # Raise an HTTPError for bad responses
                data = response.json()

                # The first element of the response is metadata, the second is the data
                if data and len(data) > 1 and data[1] is not None:
                    # Append the year to each dictionary item before collecting
                    year_data = data[1]
                    for item in year_data:
                        # Inject the year into the raw dictionary to differentiate data by year
                        item["year_id"] = year

                    all_population_data.extend(year_data)
                    print(f"Successfully fetched population data for year {year}.")
                else:
                    print(f"No data found for year {year}.")

            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from World Bank API: {e}")
                all_population_data = []

        raw_population_df = pandas.DataFrame(all_population_data)

        return raw_population_df, aggregate_codes, country_coord

    def extract_crime():
        # Define the path to our Excel file
        file_path = "./../data-sources/un-persons-convicted.xlsx"

        # Read the Excel file, specifying that the header is in row 3 (index 2)
        crime_df = pandas.read_excel(file_path, header=2)
        return crime_df

    def extract_immig():
        file_path = "./../data-sources/tps00176_linear_2_0.csv"

        df = pandas.read_csv(file_path)
        return df

    return extract_population(), extract_crime(), extract_immig()


def transform_data(raw_population_tuple, raw_crime, raw_immig):
    def transform_country_and_population(raw_population_tuple):
        population_df, aggregate_codes, coord = raw_population_tuple

        population_df = population_df[
            ~population_df["countryiso3code"].isin(aggregate_codes)
        ].copy()

        # Rename ISO3 field to match database
        population_df["country_iso3_id"] = population_df["countryiso3code"]

        # Filter invalid ISO3
        population_df = population_df[
            (population_df["country_iso3_id"].str.len() == 3) & (population_df["country_iso3_id"].notna())
        ]

        # Extract country name from subfield and convert to the lowercase (where country exists)
        population_df = population_df[population_df["country"].notna()]
        population_df = population_df[
            population_df["country"].apply(lambda x: x.get("value") is not None)
        ]

        # Create country_name from normalised country
        population_df["country_name"] = population_df["country"].apply(
            lambda x: normalise_country(x["value"]).strip().lower()
        )

        # Convert population to numeric (normalise numbers)
        population_df["population"] = pandas.to_numeric(
            population_df["value"], errors="coerce"
        )
        population_df = population_df[population_df["population"] > 0]

        # Drop rows where conversion to numeric failed or population is None
        population_df.dropna(subset=["population"], inplace=True)

        # Round population per integer number
        population_df["population"] = population_df["population"].round(0)
        population_df["population"] = population_df["population"].astype(int)

        # Check out the year (starting with YEARS[0])
        population_df["year_id"] = population_df["year_id"].astype(int)
        population_df = population_df[population_df["year_id"] >= YEARS[0]]

        # Create database matching dataframes for country and population
        country_df = (
            population_df[["country_iso3_id", "country_name"]]
            .drop_duplicates(subset=["country_iso3_id"])
            .copy()
        )
        population_df = population_df[
            ["population", "country_iso3_id", "year_id"]
        ].copy()

        c_df = country_df.join(coord.set_index("country_name"), on="country_name")

        print("Successfully transformed country and population data.")

        return c_df, population_df

    def transform_crime(crime_df):
        # Check if data that is supposed to be numerical is numerical indeed
        crime_df["VALUE"] = pandas.to_numeric(crime_df["VALUE"], errors="coerce")
        crime_df = crime_df[crime_df["VALUE"] >= 0]
        crime_df = crime_df[crime_df["VALUE"].notna()]
        # Drop rows where conversion to numeric failed or crime is None
        crime_df.dropna(subset=["VALUE"], inplace=True)

        # Rename fields to match our standard
        crime_df = crime_df.rename(
            columns={
                "Iso3_code": "country_iso3_id",
                "Country": "country_name",
                "Year": "year_id",
            }
        )

        # Filter invalid ISO3
        crime_df = crime_df[
            (crime_df["country_iso3_id"].str.len() == 3) & (crime_df["country_iso3_id"].notna())
        ]

        # since we do not need all the data, we set criteria for the data we need
        category_total = crime_df["Category"] == "Total"
        sex_total = crime_df["Sex"] == "Total"
        people_convicted = crime_df["Indicator"] == "Persons convicted"
        all_age = crime_df["Age"] == "Total"
        total_amount = crime_df["Unit of measurement"] == "Rate per 100,000 population"
        from_year = crime_df["year_id"] >= YEARS[0]
        region_europe = crime_df["Region"] == "Europe"

        crime_df = crime_df[
            category_total & sex_total & people_convicted & all_age & total_amount & from_year & region_europe
        ]

        # We have standard rounding of 2 decimal
        crime_df["VALUE"] = crime_df["VALUE"].round(2)

        # As we already have only Rate per 100,000 population and Persons convicted
        # we would rename the field
        crime_df = crime_df.rename(columns={"VALUE": "convicts_per_100000"})

        # Only keep the fields that would appear in our database
        crime_df = crime_df[["convicts_per_100000", "country_iso3_id", "year_id"]]

        print("Successfully transformed crime data.")

        return crime_df

    def transform_immig(raw_immig, population_df):
        immig_df = raw_immig[["geo", "TIME_PERIOD", "OBS_VALUE"]]

        cc = country_converter.CountryConverter()

        immig_df = immig_df[
            (immig_df["geo"].str.len() == 2) & (immig_df["geo"].notna())
        ]
        immig_df["country_iso3_id"] = cc.convert(names=immig_df["geo"], to="ISO3")
        immig_df["year_id"] = immig_df["TIME_PERIOD"].astype(int)
        immig_df["immigration_total"] = pandas.to_numeric(
            immig_df["OBS_VALUE"].replace(":", 0), errors="coerce"
        )
        immig_df = immig_df.dropna(subset=["immigration_total"])

        # Merge and keep only immigrants with countries that exist in transformed population
        immig_df = immig_df.merge(
            population_df,
            on=["country_iso3_id", "year_id"],
            how="inner",
            validate="many_to_one",
        )

        # Normalize immigrants per 100.000 inhabitants
        immig_df["immigration_per_100000"] = (
            immig_df["immigration_total"] / immig_df["population"]
        ) * 100000

        # Apply the rounding to two decimal places
        immig_df["immigration_per_100000"] = immig_df["immigration_per_100000"].round(2)

        immig_df = immig_df[["immigration_per_100000", "country_iso3_id", "year_id"]]

        print("Successfully transformed immigration data.")

        return immig_df

    country_df, population_df = transform_country_and_population(raw_population_tuple)
    return (
        country_df,
        population_df,
        transform_crime(raw_crime),
        transform_immig(raw_immig, population_df),
    )


# Now we have all dataframes created, just need to plot
a, b, c = extract_data()
countries, popu, crime, immig = transform_data(a, b, c)

# for y in YEARS:
#     print(crime.loc[crime["year_id"] == y])
#     df = popu
#     df["convicts_per_100000"] =


# df = countries.join(crime.set_index("country_iso3_id"), on="country_iso3_id")
# print(countries)
unnified_df = pandas.merge(
    left=immig,
    right=crime,
    how='inner',
    left_on=['country_iso3_id', 'year_id'],
    right_on=['country_iso3_id', 'year_id'],
)
unnified_df = unnified_df.set_index('country_iso3_id').drop_duplicates()


df = pandas.merge(
    left=unnified_df,
    right=countries,
    how='inner',
    left_on=['country_iso3_id'],
    right_on=['country_iso3_id'],
)

# clean df to make any number not there into a 0

df['text'] = df['country_name'] + '<br>Immigration ' + (df['immigration_per_100000']).astype(str) + '\n Crime' + df['convicts_per_100000'].astype(str)

max_crime = max(df['convicts_per_100000'].astype(float))
scale = 10

# for y in YEARS:
for y in [2018, 2019]:
    yearly_df = df.loc[df["year_id"] == y]
    yearly_df.to_csv(f"data{y}.csv")

    fig = go.Figure()
    fig.add_trace(go.Scattergeo(
        locationmode='ISO-3',
        lon=yearly_df['lon'],
        lat=yearly_df['lat'],
        text=yearly_df['text'],
        marker=dict(
            size=yearly_df['immigration_per_100000'] / scale,
            color=yearly_df['convicts_per_100000'] / max_crime,
            line_color='rgb(40,40,40)',
            line_width=0.5,
            colorscale=[(0, "white"), (1, "red")],
            sizemode='area'
        )))

    fig.update_layout(
        title_text=y,
        showlegend=True,
        geo=dict(
            scope='europe',
            landcolor='rgb(217, 217, 217)',
        )
    )

    fig.show()
