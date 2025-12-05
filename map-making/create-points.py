# we read the position of each country in the map and create the image of points for a given year

years = [2020, 2021, 2022, 2023, 2024, 2025]

countries = {}
crime = {}
immigration = {}


# read file "countries.txt" and store it in countries
def readCountries():
    countries["Spain"] = (0, 0)
    crime["Spain"][2020] = 0
    immigration["Spain"][2020] = 100


# use plotly to create points for each country reading from the database
def createImage(year):
    for i in countries.keys():
        print(f"point in {countries[i]} with size {immigration[i][year]} and color {crime[i][year]}")
