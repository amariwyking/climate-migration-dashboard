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
    fema_df = db.get_stat_var(
        conn, db.Table.COUNTY_FEMA_DATA, "FEMA_NRI", county_fips, 2023)

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

    st.plotly_chart(fig)


def migration_map(scenario, conn: Connection):
    try:
        with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
            counties = json.load(response)

        counties_data = db.get_county_metadata(conn)
        counties_data = counties_data.merge(
            db.get_population_projections_by_fips(conn),
            how='inner',
            on='COUNTY_FIPS'
        )

        fema_df = db.get_stat_var(conn, db.Table.COUNTY_FEMA_DATA, "FEMA_NRI",
                                  county_fips=counties_data['COUNTY_FIPS'].tolist(), year=2023)

        counties_data = counties_data.merge(
            fema_df, how="inner", on="COUNTY_FIPS")

        counties_data['geometry'] = counties_data['geometry'].apply(wkt.loads)

        # Get centroids of geometries for marker placement
        counties_data['CENTROID_LON'] = counties_data['geometry'].apply(
            lambda geom: geom.centroid.x)
        counties_data['CENTROID_LAT'] = counties_data['geometry'].apply(
            lambda geom: geom.centroid.y)

        # Calculate variation between scenario1 and the selected scenario
        # Absolute difference
        counties_data['VARIATION'] = counties_data[scenario] - \
            counties_data['POPULATION_2065_S1']

        # Percentage difference (optional)
        counties_data['VARIATION_PCT'] = ((counties_data[scenario] - counties_data['POPULATION_2065_S1']) /
                                          counties_data['POPULATION_2065_S1']) * 100

        # Apply min-max scaling to normalize population values for marker size
        min_pop = counties_data[scenario].min()
        max_pop = counties_data[scenario].max()
        counties_data['NORMALIZED_POP'] = (
            counties_data[scenario] - min_pop) / (max_pop - min_pop)

        counties_data['NRI_BUCKET'] = pd.cut(
            counties_data['FEMA_NRI'],
            bins=[0, 20, 40, 60, 80, 100],
            labels=['Very Low', 'Low', 'Moderate', 'High', 'Very High'],
            include_lowest=True
        )

        msa_counties = db.get_cbsa_counties(conn, 'metro')

        msa_data = counties_data[counties_data['COUNTY_FIPS'].isin(
            msa_counties['COUNTY_FIPS'])]

        max_abs_variation = max(
            abs(msa_data['VARIATION_PCT'].min()),
            abs(msa_data['VARIATION_PCT'].max())
        )

        # Create choropleth base layer with county boundaries
        fig = px.choropleth(
            counties_data,
            geojson=counties,
            color='CLIMATE_REGION',
            color_discrete_sequence=[
                '#8b8d6f', '#576b80', '#534e58', '#3c232e', '#c09172'],
            locations='COUNTY_FIPS',
            scope="usa",
            labels={scenario: 'Population'},
            basemap_visible=False,
            hover_data=None,
        )

        # Optionally, you can update the legend title
        fig.update_layout(
            # font_size=30,
            title=dict(
                text="County Population Gain w/ FEMA National Risk Index",
                automargin=True,
                x=0.5,  # Center the title (0 = left, 1 = right)
                y=0.95  # Adjust vertical position
            ),
            legend=dict(
                title="",
                itemsizing="constant",
                groupclick="toggleitem",
                tracegroupgap=20,  # Add space between legend groups
            ),
            margin=dict(t=100, b=50, l=50, r=50),
        )

        fig.update_layout(
            legend=dict(
                yanchor="top",
                y=0.8,
                xanchor="left",
                x=1.01,
                orientation="v"
            )
        )

        # Create the scatter_geo trace
        scatter_fig = px.scatter_geo(
            msa_data,
            lat='CENTROID_LAT',
            lon='CENTROID_LON',
            size='NORMALIZED_POP',
            size_max=70,
            color='NRI_BUCKET',
            color_discrete_sequence=[
                '#ADD8E6', '#90EE90', '#FFA500', '#FF69B4', '#D1001B'],
            custom_data=['COUNTY_FIPS', scenario,
                         'POPULATION_2065_S5b', 'VARIATION', 'VARIATION_PCT'],
            category_orders={'NRI_BUCKET': [
                'Very Low', 'Low', 'Moderate', 'High', 'Very High']}  # Ensure consistent order
        )

        # Add all traces from the scatter figure to the main figure
        for scatter_trace in scatter_fig.data:
            # Update the hover template for each trace
            scatter_trace.hovertemplate = (
                "FIPS: %{customdata[0]}<br>" +
                f"{scenario}: %{{customdata[1]}}<br>" +
                "Scenario 1: %{customdata[2]}<br>" +
                "Difference: %{customdata[3]:.2f}<br>" +
                "% Change: %{customdata[4]:.2f}%<br>" +
                "<extra></extra>"
            )

            # Add the trace to your existing figure
            fig.add_trace(scatter_trace)

        fig.update_geos(
            fitbounds="locations",
            bgcolor='rgba(0,0,0,0)',     # Transparent background
            projection_type='albers usa',  # Keep USA projection for proper focus
            projection_scale=0.5,
            scope="usa",
        )

        # Make county boundaries visible but subtle
        fig.update_traces(
            marker_line_width=0,
            selector=dict(type='choropleth')
        )

        fig.for_each_trace(
            lambda trace: trace.update(legendgroup=None, showlegend=True)
        )

        fig.for_each_trace(
            lambda trace: trace.update(
                legendgroup="climate_regions",
                legendgrouptitle=dict(
                    text="Climate Regions",
                ),
                showlegend=True
            ) if trace.type == 'choropleth' else None
        )

        # Update the scatter traces to use a different legend group
        fig.for_each_trace(
            lambda trace: trace.update(
                legendgroup="risk_levels",
                legendgrouptitle=dict(
                    text="FEMA National Risk Index",
                ),
                showlegend=True,
                marker=dict(
                    size=trace.marker.size,  # Keep existing size
                    color=trace.marker.color,  # Keep existing color
                    line=dict(width=0, color='black'),
                )
            ) if trace.type == 'scattergeo' else None
        )

        # fig.write_image('/Users/amarigarrett/Developer/climate-migration-dashboard/plot.png',
        #                 format='png', scale=2, width=2000, height=1200)

        event = st.plotly_chart(fig, on_select="ignore",
                                selection_mode=["points"])

        return event
    except Exception as e:
        print(f"Could not connect to url.\n{e}")
