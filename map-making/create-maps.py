# we read the position of each country in the map and create the image of points for a given year
import plotly.graph_objects as go

import pandas as pd

years = [2018, 2019, 2020, 2021, 2022, 2023]

countries = {}
crime = {}
immigration = {}
df = pd.read_csv("data.csv")
df.head()

df['text'] = df['name'] + '<br>Immigration ' + (df['immigration'] / 1e6).astype(str) + '\n Crime' + df['crime'].astype(str)
limits = [(0, 3), (3, 11), (11, 21), (21, 50), (50, 3000)]
colors = ["royalblue", "crimson", "lightseagreen", "orange", "lightgrey"]
cities = []
scale = 5000

fig = go.Figure()

for i in range(len(limits)):
    lim = limits[i]
    df_sub = df[lim[0]:lim[1]]
    fig.add_trace(go.Scattergeo(
        locationmode='ISO-3',
        lon=df_sub['lon'],
        lat=df_sub['lat'],
        text=df_sub['text'],
        marker=dict(
            size=df_sub['immigration'] / scale,
            color=colors[i],
            line_color='rgb(40,40,40)',
            line_width=0.5,
            sizemode='area'
        ),
        name='{0} - {1}'.format(lim[0], lim[1])))

fig.update_layout(
    title_text='Random data I made up',
    showlegend=True,
    geo=dict(
        scope='europe',
        landcolor='rgb(217, 217, 217)',
    )
)

fig.show()
