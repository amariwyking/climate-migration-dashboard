import streamlit as st
import pandas as pd
import src.utils as utils
import src.db as db


def display_population_projections(county_name, state_name, county_fips, population_historical, population_projections):
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
        scenario_data['2065'] = projected_pop_2065

        # Add this scenario to the main dictionary
        projections_dict[label] = scenario_data

        # Convert the dictionary to a DataFrame with scenarios as the index
    projection_df = pd.DataFrame(projections_dict)

    # Drop the COUNTY_FIPS column which would otherwise be included as a datapoint on the x-axis
    projection_df = projection_df.drop(index='COUNTY_FIPS')
    projection_df = projection_df.set_index(
        pd.to_datetime(projection_df.index, format='%Y'))

    # Create the chart
    st.line_chart(projection_df)

    display_migration_impact_analysis(projections_dict)


def display_migration_impact_analysis(projections_dict):
    impact_map = {
        "Scenario S5b": "Low",
        "Scenario S5a": "Medium",
        "Scenario S5c": "High"
    }
    
    # Add scenario selection dropdown
    st.write("### Impact Analysis")
    selected_scenario = st.selectbox(
        "Select a climate migration scenario:",
        # Exclude Scenario S3 (baseline)
        options=list(projections_dict.keys())[1:],
        format_func=lambda sel: impact_map.get(sel),
        index=0
    )

    # Calculate metrics based on selected scenario vs baseline
    baseline_pop_2065 = projections_dict["Scenario S3"]["2065"]
    selected_pop_2065 = projections_dict[selected_scenario]["2065"]

    # Calculate additional residents (difference between selected scenario and baseline)
    additional_residents = int(selected_pop_2065 - baseline_pop_2065)

    # Calculate percentage increase relative to baseline
    percent_increase = round(
        (additional_residents / baseline_pop_2065) * 100, 1)

    # Determine impact level based on selected scenario
    
    impact_level = impact_map.get(selected_scenario, "Unknown Impact")

    # Create columns for metrics display
    col1, col2, col3 = st.columns(3)

    # Display metrics in columns
    with col2:
        st.metric(
            label="Estimated Population by 2065",
            value=f"{selected_pop_2065:,}",
            delta=None if additional_residents == 0 else (
                f"{additional_residents:,.0f}" if additional_residents > 0 else f"{additional_residents:,.0f}")
        )

    with col3:
        st.metric(
            label="Population Increase",
            value=f"{percent_increase}%",
        )

    with col1:
        st.metric(label="Climate Impact Scenario", value=impact_level)

    # Main headline below metrics
    # st.markdown(f"""
    # ### Climate change is expected to significantly impact {county_name}'s population by 2065
    # """)


def display_housing_analysis(county_name, state_name, county_fips, db_conn):
    st.header('Housing Analysis', divider=True)

    st.write(f"### Median Gross Rent for {county_name}, {state_name}")
    st.line_chart(db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_HOUSING_DATA, "MEDIAN_GROSS_RENT", county_fips=county_fips))

    st.write(f"### Median House Value for {county_name}, {state_name}")
    st.line_chart(db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_HOUSING_DATA, "MEDIAN_HOUSING_VALUE", county_fips=county_fips))

    st.write(f"### Total Housing Units for {county_name}, {state_name}")
    st.line_chart(db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_HOUSING_DATA, "TOTAL_HOUSING_UNITS", county_fips=county_fips))

    st.write(f"### Occupied Housing Units for {county_name}, {state_name}")
    st.line_chart(db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_HOUSING_DATA, "OCCUPIED_HOUSING_UNTIS", county_fips=county_fips))


st.header('Climate Migration Dashboard')

# Get the database connection
db_conn = db.get_db_connection()

counties = db.get_county_metadata(db_conn)

# TODO: Can we package the county name and FIPS code in the selectbox?
county = st.selectbox(
    'Select a county',
    counties.NAME,
    placeholder='Type to search...',
    index=None
)

# Get the County FIPS code, which will be used for all future queries
if county:
    # Separate the county and state names
    county_name, state_name = county.split(', ')

    # Ensure that the final form of the FIPS code is 5 digits
    county_fips = str(counties[counties.NAME == county].index[0]).zfill(5)

else:
    county_name = state_name = county_fips = None

if county_fips:
    population_historical = db.get_population_timeseries(
        db_conn, None
    )

    population_projections = db.get_population_projections_by_fips(
        db_conn, None)

    if not population_projections.empty:
        display_population_projections(
            county_name, state_name, county_fips, population_historical, population_projections)

    display_housing_analysis(county_name, state_name, county_fips, db_conn)
else:
    st.info("Please select a county to view population projections.")
