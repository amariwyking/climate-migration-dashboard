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
    population_projections = db.get_population_projections_by_fips(
        db_conn, None)

    if not population_projections.empty:
        st.write(f"### Population Projections for {county_name}, {state_name}")

        county_projections = population_projections[population_projections['COUNTY_FIPS'] == int(
            county_fips)]

        print(county_projections.columns)
        print(county_projections)

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

        # Get the base 2010 population
        base_population = county_projections['POPULATION_2010'].iloc[0]

        # Add the 2010 population and each 2065 scenario as separate columns
        projection_data['2010 Population'] = [
            base_population, None]  # Just to show the starting point

        for scenario, label in zip(scenarios, scenario_labels):
            projection_data[label] = [base_population,
                                    county_projections[scenario].iloc[0]]

        st.line_chart(projection_data)
else:
    st.info("Please select a county to view population projections.")
