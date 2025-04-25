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
    fema_df = database.get_stat_var(
        Table.COUNTY_FEMA_DATA, "FEMA_NRI", county_fips, 2023)

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


def fema_nri_map(scenario):
    try:
        # Load county GeoJSON data
        with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
            counties = json.load(response)

        # Load states GeoJSON data
        with urlopen('https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json') as response:
            states_json = json.load(response)

        # Extract the features list directly
        states_features = states_json

        # Get county data and merge with population projections
        counties_data = database.get_county_metadata()
        counties_data = counties_data.merge(
            database.get_population_projections_by_fips(),
            how='inner',
            on='COUNTY_FIPS'
        )

        # Get FEMA risk data
        fema_df = database.get_stat_var(Table.COUNTY_FEMA_DATA, "FEMA_NRI",
                                county_fips=counties_data['COUNTY_FIPS'].tolist(), year=2023)

        # Merge FEMA data with counties data
        counties_data = counties_data.merge(
            fema_df, how="inner", on="COUNTY_FIPS")

        # Convert WKT to geometry objects
        counties_data['geometry'] = counties_data['geometry'].apply(wkt.loads)

        # Get centroids of geometries for marker placement
        counties_data['CENTROID_LON'] = counties_data['geometry'].apply(
            lambda geom: geom.centroid.x)
        counties_data['CENTROID_LAT'] = counties_data['geometry'].apply(
            lambda geom: geom.centroid.y)

        # Calculate variation between scenario and baseline
        counties_data['VARIATION'] = counties_data[scenario] - \
            counties_data['POPULATION_2065_S3']

        # Percentage difference
        counties_data['VARIATION_PCT'] = ((counties_data[scenario] - counties_data['POPULATION_2065_S3']) /
                                        counties_data['POPULATION_2065_S3']) * 100

        # Apply min-max scaling to normalize population values for marker size
        min_pop = counties_data[scenario].min()
        max_pop = counties_data[scenario].max()
        counties_data['NORMALIZED_POP'] = (
            counties_data[scenario] - min_pop) / (max_pop - min_pop)

        # Create NRI risk buckets
        counties_data['NRI_BUCKET'] = pd.cut(
            counties_data['FEMA_NRI'],
            bins=[0, 20, 40, 60, 80, 100],
            labels=['Very Low', 'Low', 'Moderate', 'High', 'Very High'],
            include_lowest=True
        )

        # Convert to GeoDataFrame for spatial operations
        counties_data = gpd.GeoDataFrame(counties_data)

        # Extract the state FIPS from county FIPS (first 2 digits)
        # Ensure it stays as a string
        counties_data['STATE_FIPS'] = counties_data['COUNTY_FIPS'].str[:2]

        # Check if CLIMATE_REGION exists in the dataframe
        if 'CLIMATE_REGION' not in counties_data.columns:
            st.error("Climate region data not available")
            return None

        # For each state, determine the dominant climate region by most common value
        state_climate_regions = counties_data.groupby('STATE_FIPS')['CLIMATE_REGION'].agg(
            lambda x: x.value_counts().index[0] if len(x) > 0 else None
        ).reset_index()

        # Remove any rows where CLIMATE_REGION is None
        state_climate_regions = state_climate_regions[state_climate_regions['CLIMATE_REGION'].notna(
        )]

        # Create a dictionary to map state FIPS to climate regions
        # Ensure keys remain as strings
        state_region_dict = dict(zip(state_climate_regions['STATE_FIPS'],
                                     state_climate_regions['CLIMATE_REGION']))

        # Create a list to store modified features
        modified_features = []

        # Process each state feature
        for feature in states_features['features']:
            # Ensure the id is treated as a string
            state_fips = feature['id']

            # Only include states that have climate region data
            if state_fips in state_region_dict:
                # Add climate region to the feature properties
                if 'properties' not in feature:
                    feature['properties'] = {}

                feature['properties']['CLIMATE_REGION'] = state_region_dict[state_fips]
                modified_features.append(feature)

        # Create a GeoDataFrame from the modified features
        states_gdf = gpd.GeoDataFrame.from_features(modified_features)

        # Ensure 'id' column is treated as string if it exists in the GeoDataFrame
        if 'id' in states_gdf.columns:
            states_gdf['id'] = states_gdf['id'].astype(str)

        # Dissolve states by climate region
        # This will create one geometry per unique climate region
        climate_regions_gdf = states_gdf.dissolve(by='CLIMATE_REGION')

        # Clean up geometries to remove internal boundaries
        for idx, row in climate_regions_gdf.iterrows():
            if row.geometry.geom_type == 'MultiPolygon':
                cleaned_geom = unary_union(row.geometry)
                climate_regions_gdf.at[idx, 'geometry'] = cleaned_geom

        # Convert to GeoJSON format for Plotly
        climate_regions_geojson = json.loads(climate_regions_gdf.to_json())

        # Get MSA counties for additional filtering if needed
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
            labels={
                'COUNTY_NAME': 'County',
                'CLIMATE_REGION': 'Climate Region',
                'FEMA_NRI': 'National Risk Index',
                scenario: 'Population'
            },
            basemap_visible=False,
            hover_data={
                'COUNTY_NAME': True,
                'CLIMATE_REGION': True,
                'FEMA_NRI': True,
                'COUNTY_FIPS': False  # Hide FIPS code from hover
            },
            custom_data=['COUNTY_NAME', 'CLIMATE_REGION', 'FEMA_NRI']
        )

        # Update hover template to format the display nicely
        fig.update_traces(
            hovertemplate='<b>%{customdata[0]}</b><br>' +
                        'Climate Region: %{customdata[1]}<br>' +
                        'National Risk Index: %{customdata[2]:.1f}<br>' +
                        '<extra></extra>'  # Removes trace name from hover
        )

        # Add climate regions overlay with white borders
        fig.add_trace(
            go.Choropleth(
                geojson=climate_regions_geojson,
                # Convert index to list to ensure string format
                locations=climate_regions_gdf.index.tolist(),
                z=[1] * len(climate_regions_gdf),  # Dummy values for coloring
                # Transparent fill
                colorscale=[[0, 'rgba(0,0,0,0)'], [1, 'rgba(0,0,0,0)']],
                marker_line_color='white',  # Border color for regions
                marker_line_width=5,        # Thicker border for visibility
                showscale=False,            # Hide the colorbar for this layer
                name="Climate Regions",
                showlegend=False,
                hoverinfo='skip'
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

        event = st.plotly_chart(fig, on_select="ignore",
                                selection_mode=["points"])

        return event
    except Exception as e:
        st.error(f"Could not create map: {e}")
        print(f"Could not connect to url or create map.\n{e}")
        return None