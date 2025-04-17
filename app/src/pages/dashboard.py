import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import src.db as db

from src.components import (
    vertical_spacer,
    split_row,
    migration_map,
    national_risk_score,
    climate_hazards,
)


def display_population_projections(county_name, state_name, county_fips, population_historical, population_projections):
    st.write(f"### Population Projections for {county_name}, {state_name}")

    county_pop_historical = population_historical[population_historical['COUNTY_FIPS'] == county_fips]

    # If the county has multiple rows of data, select the row with the most complete data
    if county_pop_historical.shape[0] > 1:
        # Count the number of missing values in each row
        missing_counts = county_pop_historical.isna().sum(axis=1)

        # Get the index of the row with the minimum number of missing values
        min_missing_idx = missing_counts.idxmin()

        county_pop_historical = county_pop_historical.loc[min_missing_idx]

    county_pop_projections = population_projections[population_projections['COUNTY_FIPS'] == county_fips]

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
        db_conn, db.Table.COUNTY_HOUSING_DATA, "OCCUPIED_HOUSING_UNITS", county_fips=county_fips))


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
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "BACHELORS_OR_HIGHER_TOTAL", county_fips=county_fips)
    total_pop_25_64_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "TOTAL_POPULATION_25_64", county_fips=county_fips)

    # Combine all dataframes into one
    final_df = pd.DataFrame()
    final_df["Year"] = less_than_hs_df.index
    final_df["LESS_THAN_HIGH_SCHOOL_TOTAL"] = less_than_hs_df.values
    final_df["HIGH_SCHOOL_GRADUATE_TOTAL"] = hs_graduate_df.values
    final_df["SOME_COLLEGE_TOTAL"] = some_college_df.values
    final_df["BACHELORS_OR_HIGHER_TOTAL"] = bachelors_higher_df.values
    final_df["TOTAL_POPULATION_25_64"] = total_pop_25_64_df.values

    # Calculate percentages
    final_df["LessThanHighSchool_Perc"] = (
        final_df["LESS_THAN_HIGH_SCHOOL_TOTAL"] / final_df["TOTAL_POPULATION_25_64"]) * 100
    final_df["HighSchoolGraduate_Perc"] = (
        final_df["HIGH_SCHOOL_GRADUATE_TOTAL"] / final_df["TOTAL_POPULATION_25_64"]) * 100
    final_df["SomeCollege_Perc"] = (
        final_df["SOME_COLLEGE_TOTAL"] / final_df["TOTAL_POPULATION_25_64"]) * 100
    final_df["BachelorsOrHigher_Perc"] = (
        final_df["BACHELORS_OR_HIGHER_TOTAL"] / final_df["TOTAL_POPULATION_25_64"]) * 100

    # Create a title for the chart
    st.write(
        f"### School Attainment Rate vs. Total Workforce in {county_name}, {state_name}")

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
    fig.update_yaxes(
        title_text="Percentage of Population (25-64)", secondary_y=False)
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
        # Increased bottom margin to accommodate legend
        margin=dict(l=40, r=40, t=40, b=100),
        autosize=True,
    )

    # Display the chart
    st.plotly_chart(fig, use_container_width=True)


def display_unemployment_indicators(county_name, state_name, county_fips, db_conn):
    st.header('Unemployment Analysis', divider=True)

    # Retrieve the unemployment data needed for the chart
    # Using the same pattern as your education function but with economic data table
    total_labor_force_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_ECONOMIC_DATA, "TOTAL_LABOR_FORCE", county_fips=county_fips)
    unemployed_persons_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_ECONOMIC_DATA, "UNEMPLOYED_PERSONS", county_fips=county_fips)
    unemployment_rate_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_ECONOMIC_DATA, "UNEMPLOYMENT_RATE", county_fips=county_fips)

    # Combine all dataframes into one
    total_unemployment = pd.DataFrame()
    total_unemployment["Year"] = total_labor_force_df.index
    total_unemployment["TotalLaborForce"] = total_labor_force_df.values
    total_unemployment["TotalUnemployed"] = unemployed_persons_df.values
    total_unemployment["UnemploymentRate"] = unemployment_rate_df.values

    # Create a title for the chart
    st.write(
        f"### Total Labor Force, Unemployed Population, and Unemployment Rate (2011-2023) in {county_name}, {state_name}")

    # Create a figure with secondary y-axis using Plotly
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add trace for Total Labor Force (left y-axis)
    fig.add_trace(
        go.Scatter(x=total_unemployment["Year"], y=total_unemployment["TotalLaborForce"],
                   mode="lines+markers", name="Total Labor Force",
                   line=dict(color="blue"),
                   marker=dict(symbol="circle", color="blue")),
        secondary_y=False
    )

    # Add trace for Total Unemployed (left y-axis)
    fig.add_trace(
        go.Scatter(x=total_unemployment["Year"], y=total_unemployment["TotalUnemployed"],
                   mode="lines+markers", name="Total Unemployed",
                   line=dict(color="red"),
                   marker=dict(symbol="square", color="red")),
        secondary_y=False
    )

    # Add trace for Unemployment Rate (right y-axis)
    fig.add_trace(
        go.Scatter(x=total_unemployment["Year"], y=total_unemployment["UnemploymentRate"],
                   mode="lines+markers", name="Unemployment Rate (%)",
                   line=dict(dash="dash", color="green"),
                   marker=dict(symbol="triangle-up", color="green")),
        secondary_y=True
    )

    # Set axis titles
    fig.update_xaxes(title_text="Year")
    fig.update_yaxes(title_text="Number of People", secondary_y=False)
    fig.update_yaxes(title_text="Unemployment Rate (%)",
                     secondary_y=True, color="green")

    # Update layout to match the matplotlib style
    fig.update_layout(
        xaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)'),
        yaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)'),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5
        ),
        margin=dict(l=40, r=40, t=40, b=100),
        autosize=True,
    )

    # Display the chart
    st.plotly_chart(fig, use_container_width=True)


def display_unemployment_by_education(county_name, state_name, county_fips, db_conn):
    st.header('Unemployment by Education Level', divider=True)

    # Retrieve raw counts for each education level - both unemployed and total population
    # Unemployed counts
    less_than_hs_unemployed_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "LESS_THAN_HIGH_SCHOOL_UNEMPLOYED", county_fips=county_fips)
    hs_graduate_unemployed_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "HIGH_SCHOOL_GRADUATE_UNEMPLOYED", county_fips=county_fips)
    some_college_unemployed_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "SOME_COLLEGE_UNEMPLOYED", county_fips=county_fips)
    bachelors_higher_unemployed_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "BACHELORS_OR_HIGHER_UNEMPLOYED", county_fips=county_fips)

    # Total population counts
    less_than_hs_total_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "LESS_THAN_HIGH_SCHOOL_TOTAL", county_fips=county_fips)
    hs_graduate_total_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "HIGH_SCHOOL_GRADUATE_TOTAL", county_fips=county_fips)
    some_college_total_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "SOME_COLLEGE_TOTAL", county_fips=county_fips)
    bachelors_higher_total_df = db.get_county_timeseries_data(
        db_conn, db.Table.COUNTY_EDUCATION_DATA, "BACHELORS_OR_HIGHER_TOTAL", county_fips=county_fips)

    # Combine all dataframes into one
    unemployment_by_edulevel = pd.DataFrame()
    unemployment_by_edulevel["Year"] = less_than_hs_unemployed_df.index

    # Store raw counts
    unemployment_by_edulevel["LessThanHighSchool_Unemployed"] = less_than_hs_unemployed_df.values
    unemployment_by_edulevel["HighSchoolGraduate_Unemployed"] = hs_graduate_unemployed_df.values
    unemployment_by_edulevel["SomeCollege_Unemployed"] = some_college_unemployed_df.values
    unemployment_by_edulevel["BachelorsOrHigher_Unemployed"] = bachelors_higher_unemployed_df.values

    unemployment_by_edulevel["LessThanHighSchool_Total"] = less_than_hs_total_df.values
    unemployment_by_edulevel["HighSchoolGraduate_Total"] = hs_graduate_total_df.values
    unemployment_by_edulevel["SomeCollege_Total"] = some_college_total_df.values
    unemployment_by_edulevel["BachelorsOrHigher_Total"] = bachelors_higher_total_df.values

    # Calculate unemployment rates by dividing unemployed by total population
    unemployment_by_edulevel["LessThanHighSchool_UnemploymentRate"] = (
        unemployment_by_edulevel["LessThanHighSchool_Unemployed"] /
        unemployment_by_edulevel["LessThanHighSchool_Total"] * 100
    )

    unemployment_by_edulevel["HighSchoolGraduate_UnemploymentRate"] = (
        unemployment_by_edulevel["HighSchoolGraduate_Unemployed"] /
        unemployment_by_edulevel["HighSchoolGraduate_Total"] * 100
    )

    unemployment_by_edulevel["SomeCollege_UnemploymentRate"] = (
        unemployment_by_edulevel["SomeCollege_Unemployed"] /
        unemployment_by_edulevel["SomeCollege_Total"] * 100
    )

    unemployment_by_edulevel["BachelorsOrHigher_UnemploymentRate"] = (
        unemployment_by_edulevel["BachelorsOrHigher_Unemployed"] /
        unemployment_by_edulevel["BachelorsOrHigher_Total"] * 100
    )

    # Create a title for the chart
    st.write(
        f"### Unemployment Rate by Education Level (2011-2023) in {county_name}, {state_name}")

    # Create a figure using Plotly
    fig = go.Figure()

    # Add traces for each education level's unemployment rate
    fig.add_trace(
        go.Scatter(x=unemployment_by_edulevel["Year"],
                   y=unemployment_by_edulevel["LessThanHighSchool_UnemploymentRate"],
                   mode="lines+markers",
                   name="Less Than High School",
                   marker=dict(symbol="circle"))
    )

    fig.add_trace(
        go.Scatter(x=unemployment_by_edulevel["Year"],
                   y=unemployment_by_edulevel["HighSchoolGraduate_UnemploymentRate"],
                   mode="lines+markers",
                   name="High School Graduate",
                   marker=dict(symbol="square"))
    )

    fig.add_trace(
        go.Scatter(x=unemployment_by_edulevel["Year"],
                   y=unemployment_by_edulevel["SomeCollege_UnemploymentRate"],
                   mode="lines+markers",
                   name="Some College or Associate's Degree",
                   marker=dict(symbol="triangle-up"))
    )

    fig.add_trace(
        go.Scatter(x=unemployment_by_edulevel["Year"],
                   y=unemployment_by_edulevel["BachelorsOrHigher_UnemploymentRate"],
                   mode="lines+markers",
                   name="Bachelor's Degree or Higher",
                   marker=dict(symbol="diamond"))
    )

    # Set axis titles and layout
    fig.update_xaxes(title_text="Year")
    fig.update_yaxes(title_text="Unemployment Rate (%)")

    fig.update_layout(
        xaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)'),
        yaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)'),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5
        ),
        margin=dict(l=40, r=40, t=40, b=100),
        autosize=True,
    )

    # Display the chart
    st.plotly_chart(fig, use_container_width=True)


####################################################################################################
####################################################################################################
####################################################################################################
######################################## START OF DASHBOARD ########################################
####################################################################################################
####################################################################################################
####################################################################################################


st.title('Is America Ready to Move?')

# Get the database connection
db_conn = db.get_db_connection()

counties = db.get_county_metadata(db_conn)

# Short paragraph explaining why climate migration will occur and how
st.markdown("""
### Climate-Induced Migration
Climate change is increasingly driving population shifts across the United States. As extreme weather events become more frequent and severe, communities around the country face challenges including sea-level rise, extreme heat, drought, wildfires, and flooding. These environmental pressures are expected to force increasingly more people to relocate from high-risk areas to regions with better climate resilience, impacting local economies, housing markets, and public services.
""")

# Climate migration choropleth of US counties
migration_map(None, db_conn)


# Explain factors that will affect the magnitude of climate-induced migration
st.markdown("""
            ### Climate Vulnerability Isn't the Whole Story
            """)
st.markdown("""
            Of course, climate vulnerability won't be the only factor that drives migration decisions. While some people may consider leaving areas prone to climate hazards, research shows that economic factors like job opportunities and wages will still play a dominant role in determining if, when, and where people relocate.
            """)



with st.expander("Read more", icon=":material/article:"):
    st.markdown("""When regions experiencing population loss due to climate concerns face labor shortages, wages tend to rise, creating an economic incentive for some people to stay or even move into these areas despite climate risks. Housing prices also adjust, becoming more affordable in areas experiencing outmigration, which further complicates migration patterns. This economic "dampening effect" means that even highly climate-vulnerable counties won't see mass exoduses, as financial considerations, family ties, and community connections often outweigh climate concerns in people's decision-making process. Migration is ultimately a complex interplay of climate, economic, social, and personal factors rather than a simple response to climate vulnerability alone.""")


st.markdown("##### Key Migration Decision Factors:")

st.markdown(
    "**:material/cloud_alert: Climate Risks** - Vulnerability to climate hazards")
st.markdown(
    "**:material/house: Housing Cost** - Availability of affordable housing")
st.markdown("**:material/work: Labor Demand** - Strength of local job markets")

vertical_spacer(5)

# 4. Select a county to see how it may be impacted
st.markdown(
    "Select a county to see how it may be impacted by climate-induced migration:")

# TODO: Can we package the county name and FIPS code in the selectbox?
default_county_fips = '36029'

county = st.selectbox(
    'Select a county',
    counties.NAME,
    placeholder='Type to search...',
    index=counties.index.get_loc(default_county_fips)
)

# Get the County FIPS code, which will be used for all future queries
if county:
    # Separate the county and state names
    county_name, state_name = county.split(', ')

    # Ensure that the final form of the FIPS code is 5 digits
    county_fips = counties[counties.NAME == county].index[0]
    county_fips = str(county_fips).zfill(5)
else:
    county_name = state_name = county_fips = None

if county_fips:
    st.markdown("### Climate Risk Profile")
    
    split_row(
        lambda: national_risk_score(county_name, state_name, county_fips),
        lambda: climate_hazards(county_fips, county_name),
        [0.4, 0.6])

    vertical_spacer(10)

    # 6. Show current population and projected populations of the county
    st.markdown("""
        ### Understanding Population Projections

        The population projections shown in this dashboard represent different scenarios for how climate change might affect migration patterns and population distribution across U.S. regions by 2065.

        #### What These Scenarios Mean:

        **Baseline Scenarios**:
        - **Census Projection**: Standard population growth projections without accounting for climate effects
        - **Economic Adjustment Baseline**: Includes economic factors like wages and housing prices, but no climate migration effects

        **Climate Migration Scenarios**:
        - **Low Impact**: Represents modest climate-influenced migration (50% of projected effect)
        - **Medium Impact**: Shows the full projected climate migration effect based on research by Fan et al.
        - **High Impact**: Illustrates an intensified climate migration scenario (150% of projected effect)

        These projections help visualize how climate change could reshape population distribution across regions, with some areas experiencing population growth (Northeast, West, California) and others facing decline (South, Midwest) due to climate-related migration pressures.

        The data is derived from research on climate-induced migration patterns, which considers factors including extreme weather events, economic opportunities, and regional climate vulnerabilities.
    """)

    population_historical = db.get_population_timeseries(
        db_conn, None
    )

    population_projections = db.get_population_projections_by_fips(
        db_conn, None)

    if not population_projections.empty:
        display_population_projections(
            county_name, state_name, county_fips, population_historical, population_projections)

    # 7. Show socioeconomic indicator analyses
    st.markdown("### Socioeconomic Indicators Analysis")
    st.markdown(
        f"The following indicators show how {county_name} may be affected by projected population changes:")

    display_education_indicators(county_name, state_name, county_fips, db_conn)
    split_row(
        lambda: display_unemployment_indicators(
            county_name, state_name, county_fips, db_conn),
        lambda: display_unemployment_by_education(
            county_name, state_name, county_fips, db_conn),
        [0.5,0.5]
    )
