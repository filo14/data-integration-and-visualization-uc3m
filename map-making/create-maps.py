# we read the position of each country in the map and create the image of points for a given year
import plotly.graph_objects as go

import pandas as pd

years = [2018, 2019, 2020, 2021, 2022, 2023]

countries = {}
crime = {}
immigration = {}

# this should be a dataframe with crime, immigration and country name and pos
# right now it's fake
df = pd.read_csv("data.csv")

df['text'] = df['name'] + '<br>Immigration ' + (df['immigration'] / 1e6).astype(str) + '\n Crime' + df['crime'].astype(str)
scale = 1

max_crime = max(df['crime'].astype(float))
colors = []
for i in df['crime']:
    quantity = i / max_crime
    colors.append(quantity)

fig = go.Figure()
fig.add_trace(go.Scattergeo(
    locationmode='ISO-3',
    lon=df['lon'],
    lat=df['lat'],
    text=df['text'],
    marker=dict(
        size=df['immigration'] / scale,
        color=df['crime'] / max_crime,
        line_color='rgb(40,40,40)',
        line_width=0.5,
        colorscale=[(0, "white"), (1, "red")],
        sizemode='area'
    )))

fig.update_layout(
    title_text='Random data I made up',
    showlegend=True,
    geo=dict(
        scope='europe',
        landcolor='rgb(217, 217, 217)',
    )
)

fig.show()
