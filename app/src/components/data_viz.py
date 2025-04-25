import json
import streamlit as st
import pandas as pd
import geopandas as gpd
from shapely.ops import unary_union
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from src.db import db as database, Table

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


def national_risk_score(county_fips):
    fema_df = database.get_stat_var(Table.COUNTY_FEMA_DATA, "FEMA_NRI", county_fips, 2023)

    # Dummy NRI data for demonstration
    nri_score = fema_df["FEMA_NRI"].iloc[0]

    # Use light gray for the gauge bar
    bar_color = "rgba(255, 255, 255, 0.5)"

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

    fig.update_layout(
        # width=480,
        height=240,
        margin=dict(
            b=0,
            t=40,
            l=80,
            r=80,
        ),
        autosize=True,
        xaxis=dict(
            domain=[0, 0.95]
        )
    )

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


def migration_map(scenario):
    try:
        with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
            counties = json.load(response)

        counties_data = database.get_county_metadata()
        counties_data = counties_data.merge(
            database.get_population_projections_by_fips(),
            how='inner',
            on='COUNTY_FIPS'
        )

        fema_df = database.get_stat_var(Table.COUNTY_FEMA_DATA, "FEMA_NRI",
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

        counties_data = gpd.GeoDataFrame(counties_data)

        climate_regions_gdf = counties_data.dissolve(by='CLIMATE_REGION', )

        # Clean up the geometries to remove internal boundaries
        for idx, row in climate_regions_gdf.iterrows():
            # If the geometry is a MultiPolygon, convert it to a single Polygon
            if row.geometry.geom_type == 'MultiPolygon':
                # Use unary_union to merge all the polygons and remove internal boundaries
                cleaned_geom = unary_union(row.geometry)
                climate_regions_gdf.at[idx, 'geometry'] = cleaned_geom

        climate_regions_geojson = climate_regions_gdf.__geo_interface__

        msa_counties = database.get_cbsa_counties('metro')

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
            color='NRI_BUCKET',
            color_discrete_sequence=[
                RISK_COLORS_RGBA[3],
                RISK_COLORS_RGBA[4],
                RISK_COLORS_RGBA[2],
                RISK_COLORS_RGBA[1],
                RISK_COLORS_RGBA[0],
            ],
            locations='COUNTY_FIPS',
            scope="usa",
            labels={scenario: 'Population'},
            basemap_visible=False,
            hover_data=None,
        )

        fig.add_trace(
            go.Choropleth(
                geojson=climate_regions_geojson,
                locations=climate_regions_gdf.index,  # Using the region names as locations
                z=[1] * len(climate_regions_gdf),  # Dummy values for coloring
                colorscale=[[0, 'rgba(0,0,0,0)'], [1, 'rgba(0,0,0,0)']],
                marker_line_color='white',  # Border color for regions
                marker_line_width=5,        # Thicker border for visibility
                showscale=False,            # Hide the colorbar for this layer
                name="Climate Regions",
                showlegend=False,
            )
        )

        # Configure the map layout
        fig.update_geos(
            visible=False,
            scope="usa",
            showcoastlines=True,
            projection_type="albers usa"
        )

        fig.update_layout(
            height=800,
            title=dict(
                text="County Population Gain w/ FEMA National Risk Index",
                automargin=True,
                y=0.95  # Adjust vertical position
            ),
            legend=dict(
                title="",
                itemsizing="constant",
                groupclick="toggleitem",
                tracegroupgap=20,  # Add space between legend groups
                yanchor="top",
                y=0.9,
                xanchor="left",
                x=1.01,
                orientation="v"
            ),
            margin=dict(t=100, b=50, l=50, r=50),
            autosize=True,
        )

        # fig.write_image('/Users/amarigarrett/Developer/climate-migration-dashboard/plot.png',
        #                 format='png', scale=2, width=2000, height=1200)

        event = st.plotly_chart(fig, on_select="ignore",
                                selection_mode=["points"])

        return event
    except Exception as e:
        print(f"Could not connect to url.\n{e}")
