import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import src.db as db

import json
from urllib.request import urlopen


def migration_map(data, conn):
    try:
        with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
            counties = json.load(response)

        df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/fips-unemp-16.csv",
                        dtype={"fips": str})

        counties_data = db.get_county_metadata(conn)

        # print(counties_data)

        fig = px.choropleth(df, geojson=counties, locations='fips', color='unemp',
                            color_continuous_scale="Viridis",
                            range_color=(0, 12),
                            scope="usa",
                            labels={'unemp': 'unemployment rate'},
                            basemap_visible=False,
                            )


        fig.update_layout(
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            coloraxis_colorbar=dict(
                orientation='h',  # 'h' for horizontal
                # thickness=15,     # Adjust thickness of the colorbar
                len=0.6,          # Length as fraction of the plot area width
                y=-0.1,           # Position below the map (-0.1 means 10% below)
                x=0.5,            # Center the colorbar horizontally
                # xanchor='center',  # Anchor point for x position
                yanchor='top'     # Anchor point for y position
            ),
        )
        
        fig.update_traces(marker_line_width=0)

        event = st.plotly_chart(fig, on_select="rerun", selection_mode=[
            "points"])
    except Exception as e:
        print(f"Could not connect to url.\n{e}")

def national_risk_score(county_name, state_name, county_fips):
    # 5. Show NRI score for county and the top hazards
    st.markdown(f"### Climate Risk Profile: {county_name}, {state_name}")

    # Dummy NRI data for demonstration
    nri_score = 46.8  # Example overall risk score

    # Display the NRI score with a gauge chart
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=nri_score,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "National Risk Index Score"},
        gauge={
            'axis': {'range': [0, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 25], 'color': "lightgreen"},
                {'range': [25, 50], 'color': "yellow"},
                {'range': [50, 75], 'color': "orange"},
                {'range': [75, 100], 'color': "red"}
            ]
        }
    ))

    st.plotly_chart(fig)
    
def climate_hazards(county_fips, county_name):
    # Display top hazards
    st.markdown("#### Top Climate Hazards")

    hazard_data = {
        "Hazard Type": ["Extreme Heat", "Drought", "Riverine Flooding", "Wildfire", "Hurricane"],
        "Risk Score": [82.4, 64.7, 42.3, 37.8, 15.2]
    }

    hazards_df = pd.DataFrame(hazard_data)

    # Sort by risk score
    hazards_df = hazards_df.sort_values("Risk Score", ascending=False)

    # Create a horizontal bar chart
    fig = px.bar(
        hazards_df,
        x="Risk Score",
        y="Hazard Type",
        orientation='h',
        color="Risk Score",
        color_continuous_scale=["green", "yellow", "orange", "red"],
        title=f"Climate Hazards for {county_name}",
        labels={"Risk Score": "Risk Score (Higher = Greater Risk)"}
    )

    st.plotly_chart(fig)