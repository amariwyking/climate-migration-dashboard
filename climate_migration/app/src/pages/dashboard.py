import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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


def display_housing_indicators(county_name, state_name, county_fips, db_conn):
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


def display_education_indicators(county_name, state_name, county_fips, db_conn):
    st.header('Education Analysis', divider=True)
    
    # Retrieve all the educational attainment data needed for the chart
    less_than_hs_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "LESS_THAN_HIGH_SCHOOL_TOTAL", county_fips=county_fips)
    hs_graduate_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "HIGH_SCHOOL_GRADUATE_TOTAL", county_fips=county_fips)
    some_college_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "SOME_COLLEGE_TOTAL", county_fips=county_fips)
    bachelors_higher_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "BACHELOR_OR_HIGH_TOTAL", county_fips=county_fips)
    total_pop_25_64_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "TOTAL_POPULATION_25_64", county_fips=county_fips)
    
    # Combine all dataframes into one
    final_df = pd.DataFrame()
    final_df["Year"] = less_than_hs_df.index
    final_df["LESS_THAN_HIGH_SCHOOL_TOTAL"] = less_than_hs_df.values
    final_df["HIGH_SCHOOL_GRADUATE_TOTAL"] = hs_graduate_df.values
    final_df["SOME_COLLEGE_TOTAL"] = some_college_df.values
    final_df["BACHELOR_OR_HIGH_TOTAL"] = bachelors_higher_df.values
    final_df["TOTAL_POPULATION_25_64"] = total_pop_25_64_df.values
    
    # Calculate percentages
    final_df["LessThanHighSchool_Perc"] = (final_df["LESS_THAN_HIGH_SCHOOL_TOTAL"] / final_df["TOTAL_POPULATION_25_64"]) * 100
    final_df["HighSchoolGraduate_Perc"] = (final_df["HIGH_SCHOOL_GRADUATE_TOTAL"] / final_df["TOTAL_POPULATION_25_64"]) * 100
    final_df["SomeCollege_Perc"] = (final_df["SOME_COLLEGE_TOTAL"] / final_df["TOTAL_POPULATION_25_64"]) * 100
    final_df["BachelorsOrHigher_Perc"] = (final_df["BACHELOR_OR_HIGH_TOTAL"] / final_df["TOTAL_POPULATION_25_64"]) * 100
    
    # Create a title for the chart
    st.write(f"### School Attainment Rate vs. Total Workforce in {county_name}, {state_name}")
    
    # Since Streamlit doesn't natively support dual-axis charts, we'll use Plotly
    
    
    # Create a figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add traces for educational attainment percentages (left y-axis)
    fig.add_trace(
        go.Scatter(x=final_df["Year"], y=final_df["LessThanHighSchool_Perc"], 
                   mode="lines+markers", name="Less than High School (%)",
                   marker=dict(symbol="circle")),
        secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(x=final_df["Year"], y=final_df["HighSchoolGraduate_Perc"], 
                   mode="lines+markers", name="High School Graduate (%)",
                   marker=dict(symbol="square")),
        secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(x=final_df["Year"], y=final_df["SomeCollege_Perc"], 
                   mode="lines+markers", name="Some College or Associate's Degree (%)",
                   marker=dict(symbol="triangle-up")),
        secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(x=final_df["Year"], y=final_df["BachelorsOrHigher_Perc"], 
                   mode="lines+markers", name="Bachelor's Degree or Higher (%)",
                   marker=dict(symbol="diamond")),
        secondary_y=False
    )
    
    # Add trace for total population (right y-axis)
    fig.add_trace(
        go.Scatter(x=final_df["Year"], y=final_df["TOTAL_POPULATION_25_64"], 
                   mode="lines+markers", name="Total Population (25-64)",
                   line=dict(dash="dash", color="black"),
                   marker=dict(symbol="star", color="black")),
        secondary_y=True
    )
    
    # Set axis titles
    fig.update_xaxes(title_text="Year")
    fig.update_yaxes(title_text="Percentage of Population (25-64)", secondary_y=False)
    fig.update_yaxes(title_text="Total Population (25-64)", secondary_y=True)
    
    fig.update_layout(
        xaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)'),
        yaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)'),
        legend=dict(
            orientation="h",  # Horizontal legend
            yanchor="bottom",
            y=-0.3,  # Position below the plot
            xanchor="center",
            x=0.5    # Center the legend horizontally
        ),
        margin=dict(l=40, r=40, t=40, b=100),  # Increased bottom margin to accommodate legend
        autosize=True,
    )
    
    # Display the chart
    st.plotly_chart(fig, use_container_width=True)


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

    display_housing_indicators(county_name, state_name, county_fips, db_conn)
    
    display_education_indicators(county_name, state_name, county_fips, db_conn)
else:
    st.info("Please select a county to view population projections.")
