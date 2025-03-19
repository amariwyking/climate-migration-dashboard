import streamlit as st
import pandas as pd
import src.utils as utils
import src.db as db

# TODO: Can we package the county name and FIPS code in the selectbox?
county = st.selectbox(
    'Select a county',
    utils.get_all_county_names(),
    placeholder='Type to search...',
    index=None
)

# Get the County FIPS code, which will be used for all future queries
if county:
    # Separate the county and state names
    county_name, state_name = county.split(', ')

    # Ensure that the final form of the FIPS code is 5 digits
    county_fips = str(utils.get_county_fips_code(county)).zfill(5)

else:
    county_name = state_name = county_fips = None

# Want to display time series of population
st.header('Climate Migration Scenarios')

# Get the database connection
db_conn = db.get_db_connection()

# Call the function with the connection and FIPS code
if county_fips:
    population_historical = db.get_population_timeseries(
        db_conn, None
    )

    population_projections = db.get_population_projections_by_fips(
        db_conn, None)

    if not population_projections.empty:
        st.write(f"### Population Projections for {county_name}, {state_name}")

        county_pop_historical = population_historical[population_historical['COUNTY_FIPS'] == int(
            county_fips)]
        
        # If the county has multiple rows of data, select the row with the most complete data
        if county_pop_historical.shape[0] > 1:
            # Count the number of missing values in each row
            missing_counts = county_pop_historical.isna().sum(axis=1)

            # Get the index of the row with the minimum number of missing values
            min_missing_idx = missing_counts.idxmin()

            county_pop_historical = county_pop_historical.loc[min_missing_idx]

        county_pop_projections = population_projections[population_projections['COUNTY_FIPS'] == int(
            county_fips)]

        # TODO: Rewrite to work with any number of scenarios that are included in the projections
        
        
        # Initialize dataframe to store all scenario data
        projection_data = pd.DataFrame(index=[2010, 2065])

        scenarios = [
            'POPULATION_2065_S3',
            'POPULATION_2065_S5b',
            'POPULATION_2065_S5a',
            'POPULATION_2065_S5c',
        ]
        
        scenario_labels = [
            'Scenario S3', 
            'Scenario S5a',
            'Scenario S5b', 
            'Scenario S5c'
        ]

        # Create a dictionary to store all projection scenarios
        projections_dict = {}

        # Add each projection scenario to the dictionary
        for scenario, label in zip(scenarios, scenario_labels):
            # Get the projected 2065 population for this scenario
            projected_pop_2065 = county_pop_projections[scenario].iloc[0]
            
            # Create a copy of the historical data for this scenario
            scenario_data = county_pop_historical.copy()
            
            # Add the 2065 projection to this scenario's data
            scenario_data['pop2065'] = projected_pop_2065
            
            # Add this scenario to the main dictionary
            projections_dict[label] = scenario_data

        # Convert the dictionary to a DataFrame with scenarios as the index
        projection_df = pd.DataFrame(projections_dict)
        
        # Drop the COUNTY_FIPS column which would otherwise be included as a datapoint on the x-axis
        projection_df = projection_df.drop(index='COUNTY_FIPS')
        projection_df = projection_df.set_index(projection_df.index.str[3:])

        # Create the chart
        st.line_chart(projection_df)
else:
    st.info("Please select a county to view population projections.")
