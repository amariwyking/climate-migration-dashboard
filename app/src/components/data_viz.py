import json
from sqlalchemy import Connection
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

import src.db as db

from shapely import wkt
from urllib.request import urlopen


# Define the color palette globally to avoid duplication
RISK_COLORS_RGB = [
    (77, 109, 189),   # #4D6DBD - Range 0-20
    (80, 155, 199),   # #509BC7 - Range 20-40
    (240, 213, 93),   # #F0D55D - Range 40-60
    (224, 112, 105),  # #E07069 - Range 60-80
    (199, 68, 93),    # #C7445D - Range 80-100
]

# Generate color formats once
RISK_COLORS_RGBA = [f"rgba({r}, {g}, {b}, 1)" for r, g, b in RISK_COLORS_RGB]
RISK_COLORS_RGB_STR = [f"rgb({r}, {g}, {b})" for r, g, b in RISK_COLORS_RGB]

# Risk level labels
RISK_LEVELS = ['Very Low', 'Low', 'Moderate', 'High', 'Very High']

# Map risk categories to colors
RISK_COLOR_MAPPING = dict(zip(RISK_LEVELS, RISK_COLORS_RGB_STR))


def get_risk_color(score, opacity=1.0):
    """Get color for a risk score with specified opacity"""
    color_index = min(int(score // 20), 4)
    r, g, b = RISK_COLORS_RGB[color_index]
    return f"rgba({r}, {g}, {b}, {opacity})"


def national_risk_score(conn: Connection, county_fips):
    fema_df = db.get_stat_var(conn, db.Table.COUNTY_FEMA_DATA, "FEMA_NRI", county_fips, 2023)
    
    # Dummy NRI data for demonstration
    nri_score = fema_df["FEMA_NRI"].iloc[0]

    # Use light gray for the gauge bar
    bar_color = "rgba(100, 100, 100, 0.8)"

    # Display the NRI score with a gauge chart
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=nri_score,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "National Risk Index Score"},
        gauge={
            'axis': {'range': [0, 100]},
            'bar': {
                'thickness': 0.5,
                'color': bar_color,
            },
            'steps': [
                {'range': [0, 20], 'color': RISK_COLORS_RGBA[0]},
                {'range': [20, 40], 'color': RISK_COLORS_RGBA[1]},
                {'range': [40, 60], 'color': RISK_COLORS_RGBA[2]},
                {'range': [60, 80], 'color': RISK_COLORS_RGBA[3]},
                {'range': [80, 100], 'color': RISK_COLORS_RGBA[4]},
            ]
        }
    ))

    st.plotly_chart(fig)


def climate_hazards(county_fips, county_name):
    # Display top hazards
    hazard_data = {
        "Hazard Type": ["Extreme Heat", "Drought", "Riverine Flooding", "Wildfire", "Hurricane"],
        "Risk Score": [82.4, 64.7, 42.3, 37.8, 15.2]
    }

    hazards_df = pd.DataFrame(hazard_data)
    hazards_df = hazards_df.sort_values("Risk Score", ascending=False)

    # Create a color mapping based on risk score ranges
    hazards_df['Color Category'] = pd.cut(
        hazards_df['Risk Score'],
        bins=[0, 20, 40, 60, 80, 100],
        labels=RISK_LEVELS,
        include_lowest=True
    )

    # Create a horizontal bar chart
    fig = px.bar(
        hazards_df,
        x="Risk Score",
        y="Hazard Type",
        orientation='h',
        color="Color Category",
        color_discrete_map=RISK_COLOR_MAPPING,
        title="Climate Hazards",
        labels={"Risk Score": "Risk Score (Higher = Greater Risk)"}
    )

    # Improve layout
    fig.update_layout(
        legend_title_text="Risk Level",
        xaxis_range=[0, 100]
    )

    st.plotly_chart(fig)
    
def migration_map(data, conn: Connection):
    try:
        with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
            counties = json.load(response)

        counties_data = db.get_county_metadata(conn)
        counties_data = counties_data.merge(
            db.get_population_projections_by_fips(conn),
            how='inner',
            on='COUNTY_FIPS'
        )

        counties_data['geometry'] = counties_data['geometry'].apply(wkt.loads)

        fig = px.choropleth(
            counties_data, 
            geojson=counties, 
            locations='COUNTY_FIPS', 
            color=np.log(counties_data['POPULATION_2065_S5c']),
            color_continuous_scale="Viridis",
            scope="usa",
            labels={'POPULATION_2065_S5c': 'Population Increase'},
            basemap_visible=False
        )

        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            coloraxis_colorbar=dict(
                orientation='v',
                thickness=30,
                len=0.6,
                y=0.5,
                x=0.9,
            ),
        )
        fig.update_traces(marker_line_width=0)

        event = st.plotly_chart(fig, on_select="rerun", selection_mode=["points"])
        return event
    except Exception as e:
        print(f"Could not connect to url.\n{e}")