import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.db import db as database, Table, get_db_connection

from src.components import (
    vertical_spacer,
    split_row,
    fema_nri_map,
    population_by_climate_region,
    national_risk_score,
    climate_hazards,
    socioeconomic_projections,
    plot_socioeconomic_indices,
    plot_socioeconomic_radar,
)

def display_scenario_impact_analysis(county_name, state_name, projected_data):
    """
    Display comprehensive impact analysis based on projected data
    """
    st.header(f"Migration Impact Analysis")
    
    # Add explanation of the scenarios
    with st.expander("About the Scenarios", expanded=False):
        st.markdown("""
        ### Understanding the Scenarios
        """)
        
        # 6. Show current population and projected populations of the county
        st.markdown("""
            The population projections shown in this dashboard represent different scenarios for how climate change might affect migration patterns and population distribution across U.S. regions by 2065.

            #### What These Scenarios Mean:

        """)

        feature_cards([
            {"title": "No Impact",
                "description": "The projection model only considers labor and housing feedback mechanisms"},
            {"title": "Low Impact",
                "description": "Model includes modest climate-influenced migration (50% of projected effect)"},
            {"title": "Medium Impact",
                "description": "The expected influence of climate migration on migration decisions (100% of projected effect)"},
            {"title": "High Impact",
                "description": "Illustrates an intensified scenario where climate factors are more severe (200% of projected effect)"},
        ])
    
    # Create tabs for different impact categories
    tab1, tab2, tab3 = st.tabs(["Employment", "Education", "Housing"])
    
    with tab1:
        st.subheader("Employment Impact")
        st.markdown("""
        This chart shows how different migration scenarios could affect employment rates in your community. 
        The 4% unemployment line represents the Non-Accelerating Inflation Rate of Unemployment (NAIRU), 
        generally considered to be a healthy level of unemployment in a stable economy.
        """)
        
        # Display employment chart
        employment_chart = create_employment_chart(projected_data)
        st.plotly_chart(employment_chart, use_container_width=True)
        
        # Add interpretation based on the data
        unemployment_above_threshold = any(100 - row['TOTAL_EMPLOYED_PERCENTAGE'] > 4.0 for _, row in projected_data.iterrows())
        
        if unemployment_above_threshold:
            st.warning(":material/warning: Under some scenarios, unemployment may rise above the 4% NAIRU threshold, which could indicate economic stress.")
        else:
            st.success(":material/check_circle_outline: Employment levels remain healthy across all scenarios, suggesting economic resilience.")
    
    with tab2:
        st.subheader("Education Impact")
        st.markdown("""
        This chart displays the projected student-teacher ratios under different scenarios. 
        The national average is approximately 16:1, with higher ratios potentially indicating 
        strained educational resources.
        """)
        
        # Display education chart
        education_chart = create_student_teacher_chart(projected_data)
        st.plotly_chart(education_chart, use_container_width=True)
        
        # Add interpretation based on the data
        high_ratio_scenarios = [row['SCENARIO'] for _, row in projected_data.iterrows() if row['STUDENT_TEACHER_RATIO'] > 16.0]
        
        if high_ratio_scenarios:
            st.warning(f"⚠️ The student-teacher ratio exceeds the recommended level in {', '.join(high_ratio_scenarios)}. This may require additional educational resources or staff.")
        else:
            st.success(":material/check_circle_outline: Educational resources appear adequate across all scenarios.")
    
    with tab3:
        st.subheader("Housing Impact")
        st.markdown("""
        This visualization shows housing availability across scenarios. A healthy housing market typically 
        maintains a vacancy rate between 5-8% (occupancy rate of 92-95%). Rates outside this range may 
        indicate housing shortages or excess vacancy.
        """)
        
        # Display housing chart
        housing_chart = create_housing_chart(projected_data)
        st.plotly_chart(housing_chart, use_container_width=True)
        
        # Calculate and add interpretation
        for _, row in projected_data.iterrows():
            occupancy_rate = (row['OCCUPIED_HOUSING_UNITS'] / (row['OCCUPIED_HOUSING_UNITS'] + row['AVAILABLE_HOUSING_UNITS'])) * 100
            vacancy_rate = 100 - occupancy_rate
            
            if vacancy_rate < 5:
                st.warning(f"In the {row['SCENARIO']} scenario, the vacancy rate is below 5%, indicating a potential housing shortage.")
            elif vacancy_rate > 8:
                st.info(f"In the {row['SCENARIO']} scenario, the vacancy rate is above 8%, suggesting potential excess housing capacity.")

def generate_policy_recommendations(projected_data):
    """Generate policy recommendations based on the projected data"""
    st.write("# Policy Recommendations")
    
    # Calculate metrics for recommendations
    recommendations = []
    
    # Check employment metrics
    for _, row in projected_data.iterrows():
        unemployment_rate = 100 - row['TOTAL_EMPLOYED_PERCENTAGE']
        if unemployment_rate > 4.0 and row['SCENARIO'] in ['S5b', 'S5c']:
            recommendations.append({
                'category': 'Employment',
                'scenario': row['SCENARIO'],
                'issue': f"Projected unemployment rate of {unemployment_rate:.1f}% exceeds optimal levels",
                'recommendation': "Consider workforce development programs and economic incentives to attract industries likely to thrive in changing climate conditions."
            })
    
    # Check education metrics
    for _, row in projected_data.iterrows():
        if row['STUDENT_TEACHER_RATIO'] > 16.0 and row['SCENARIO'] in ['S5b', 'S5c']:
            recommendations.append({
                'category': 'Education',
                'scenario': row['SCENARIO'],
                'issue': f"Student-teacher ratio of {row['STUDENT_TEACHER_RATIO']:.1f} exceeds national average",
                'recommendation': "Plan for educational infrastructure expansion and teacher recruitment to maintain educational quality with population growth."
            })
    
    # Check housing metrics
    for _, row in projected_data.iterrows():
        occupancy_rate = (row['OCCUPIED_HOUSING_UNITS'] / (row['OCCUPIED_HOUSING_UNITS'] + row['AVAILABLE_HOUSING_UNITS'])) * 100
        vacancy_rate = 100 - occupancy_rate
        
        if vacancy_rate <= 0 and row['SCENARIO'] in ['S5b', 'S5c']:
            recommendations.append({
                'category': 'Housing',
                'scenario': row['SCENARIO'],
                'issue': f"Negative vacancy rate of {vacancy_rate:.1f}% indicates a shortage of housing.",
                'recommendation': "Implement zoning reforms and incentives for affordable housing development to accommodate projected population growth."
            })
        elif vacancy_rate < 5 and row['SCENARIO'] in ['S5b', 'S5c']:
            recommendations.append({
                'category': 'Housing',
                'scenario': row['SCENARIO'],
                'issue': f"Low vacancy rate of {vacancy_rate:.1f}% indicates potential housing shortage",
                'recommendation': "Implement zoning reforms and incentives for affordable housing development to accommodate projected population growth."
            })
        elif vacancy_rate > 8 and row['SCENARIO'] in ['S5b', 'S5c']:
            recommendations.append({
                'category': 'Housing',
                'scenario': row['SCENARIO'],
                'issue': f"High vacancy rate of {vacancy_rate:.1f}% indicates potential housing surplus",
                'recommendation': "Consider adaptive reuse strategies for vacant properties and focus on maintaining existing housing stock quality."
            })
    
    # Display recommendations
    if recommendations:
        for category in ['Employment', 'Education', 'Housing']:
            category_recommendations = [r for r in recommendations if r['category'] == category]
            if category_recommendations:
                st.write(f"##### {category} Recommendations")
                for rec in category_recommendations:
                    with st.expander(f"{rec['issue']} in {rec['scenario']} scenario"):
                        st.write(rec['recommendation'])
    else:
        st.info("Based on current projections, no critical interventions are needed as metrics remain within healthy ranges across scenarios.")

def create_housing_chart(projected_data):
    # Make a copy of the dataframe to avoid modifying the original
    df = projected_data.copy()
    
    # Sort the dataframe by SCENARIO
    df = df.sort_values('SCENARIO')
    
        # Get the max absolute value for symmetric axis
    max_value = max(abs(df['AVAILABLE_HOUSING_UNITS'].max()), 
                    abs(df['AVAILABLE_HOUSING_UNITS'].min()))

    # Calculate housing metrics if not already in the dataframe
    if 'HOUSING_OCCUPANCY_RATE' not in df.columns:
        df['HOUSING_OCCUPANCY_RATE'] = (df['OCCUPIED_HOUSING_UNITS'] / (df['OCCUPIED_HOUSING_UNITS'] + df['AVAILABLE_HOUSING_UNITS'])) * 100
    
    # Create the horizontal bar chart
    fig = go.Figure()
    
    # Sort the data by AVAILABLE_HOUSING_UNITS for better visualization
    sorted_data = df.sort_values('AVAILABLE_HOUSING_UNITS')

    fig.add_trace(go.Bar(
        y=sorted_data['SCENARIO'],
        x=sorted_data['AVAILABLE_HOUSING_UNITS'],
        orientation='h',
        marker=dict(
            color=sorted_data['AVAILABLE_HOUSING_UNITS'].apply(lambda x: '#E07069' if x < 0 else '#509BC7'),
            line=dict(color='rgba(0, 0, 0, 0.2)', width=1)
        )
    ))

    # Update layout for better appearance
    fig.update_layout(
        title="Projected Available Housing Units by Scenario in 2065",
        xaxis=dict(
            title="Available Housing Units in 2065",
            range=[-max_value, max_value],  # Symmetric x-axis
            zeroline=True,
            zerolinecolor='black',
            zerolinewidth=1
        ),
        yaxis=dict(
            title="Scenario",
            autorange="reversed"  # To have the largest value at the top
        ),
        height=500,
        margin=dict(l=100, r=20, t=70, b=70),
        template="plotly_white"
    )

    # Adding a vertical reference line at x=0
    fig.add_shape(
        type="line",
        x0=0, y0=-0.5,
        x1=0, y1=len(sorted_data) - 0.5,
        line=dict(color="black", width=1, dash="solid")
    )

    # Display the chart
    return fig

def create_student_teacher_chart(projected_data):
    # Make a copy of the dataframe to avoid modifying the original
    df = projected_data.copy()
    
    # Sort the dataframe by SCENARIO
    df = df.sort_values('SCENARIO')
    
    # Create figure
    fig = go.Figure()
    
    # Define the optimal student-teacher ratio threshold
    optimal_ratio = 16.0  # National average is around 16:1
    
    # Add bar for each scenario
    fig.add_trace(
        go.Bar(
            x=df['SCENARIO'],
            y=df['STUDENT_TEACHER_RATIO'],
            marker=dict(
                color=[
                    '#E07069' if ratio > optimal_ratio else '#509BC7' 
                    for ratio in df['STUDENT_TEACHER_RATIO']
                ]
            ),
            text=[f"{ratio:.1f}" for ratio in df['STUDENT_TEACHER_RATIO']],
            textposition='auto',
            hovertemplate='Student-Teacher Ratio: %{y:.1f}<extra></extra>'
        )
    )
    
    # Add threshold line
    fig.add_shape(
        type="line",
        x0=-0.5,
        y0=optimal_ratio,
        x1=len(df) - 0.5,
        y1=optimal_ratio,
        line=dict(
            color="gray",
            width=2,
            dash="dash",
        ),
    )
    
    # Add annotation for the threshold
    fig.add_annotation(
        x=len(df) - 1,
        y=optimal_ratio + 0.5,
        text="Optimal Ratio (16:1)",
        showarrow=False,
        font=dict(
            color="gray"
        )
    )
    
    # Update layout
    fig.update_layout(
        title='Projected Student-Teacher Ratio by Scenario',
        xaxis=dict(
            title='Scenario',
            tickmode='array',
            tickvals=list(range(len(df))),
            ticktext=df['SCENARIO']
        ),
        yaxis=dict(
            title='Student-Teacher Ratio',
            range=[0, max(df['STUDENT_TEACHER_RATIO']) * 1.2]  # Add some padding
        ),
        margin=dict(l=50, r=50, t=80, b=50),
        height=400,
    )
    
    return fig

def format_percentage(percentage):
    return f"{percentage:.1f}%"

def create_employment_chart(projected_data):
    # Make a copy of the dataframe to avoid modifying the original
    df = projected_data.copy()
    
    # Calculate the unemployed percentage for each scenario
    df['UNEMPLOYED_PERCENTAGE'] = 100 - df['TOTAL_EMPLOYED_PERCENTAGE']
    
    # Sort the dataframe by SCENARIO
    df = df.sort_values('SCENARIO')
    
    # Define the NAIRU threshold
    nairu_threshold = 4.0
    
    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add traces for employed and unemployed percentages
    for index, row in df.iterrows():
        # Determine color for unemployed percentage bar
        unemployed_color = '#E07069' if row['UNEMPLOYED_PERCENTAGE'] > nairu_threshold else '#F0D55D'
        
        # Add employed percentage bar
        fig.add_trace(
            go.Bar(
                name='Employed',
                y=[row['SCENARIO']],
                x=[row['TOTAL_EMPLOYED_PERCENTAGE']],
                orientation='h',
                marker=dict(color='#509BC7'),
                text=[format_percentage(row['TOTAL_EMPLOYED_PERCENTAGE'])],
                textposition='inside',
                hoverinfo='text',
                hovertext=[f"Employed: {format_percentage(row['TOTAL_EMPLOYED_PERCENTAGE'])}"],
                showlegend=index == 0  # Only show in legend for the first entry
            )
        )
        
        # Add unemployed percentage bar
        fig.add_trace(
            go.Bar(
                name='Unemployed',
                y=[row['SCENARIO']],
                x=[row['UNEMPLOYED_PERCENTAGE']],
                orientation='h',
                marker=dict(color=unemployed_color),
                text=[format_percentage(row['UNEMPLOYED_PERCENTAGE'])],
                textposition='inside',
                hoverinfo='text',
                hovertext=[f"Unemployed: {format_percentage(row['UNEMPLOYED_PERCENTAGE'])}"],
                showlegend=index == 0  # Only show in legend for the first entry
            )
        )
    
    # Add NAIRU threshold line
    fig.add_trace(
        go.Scatter(
            name='NAIRU Threshold (4%)',
            x=[nairu_threshold],
            y=df['SCENARIO'],
            mode='lines',
            line=dict(color='gray', width=2, dash='dash'),
            opacity=0.8,
            hoverinfo='text',
            hovertext=['NAIRU Threshold: 4%'],
            showlegend=True
        ),
        secondary_y=False
    )
    
    # Update layout
    fig.update_layout(
        title='Projected Employment by Scenario',
        barmode='stack',
        xaxis=dict(
            title='Percentage (%)',
            range=[0, 100],
            tickvals=[0, 20, 40, 60, 80, 100],
            ticktext=['0%', '20%', '40%', '60%', '80%', '100%']
        ),
        yaxis=dict(
            title='Scenario',
            categoryorder='array',
            categoryarray=df['SCENARIO'].tolist()
        ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        ),
        margin=dict(l=50, r=50, t=80, b=50),
        height=400,
    )
    
    return fig

def feature_cards(items):
    """
    Display a grid of feature cards with material icons, titles, and descriptions.

    Parameters:
    - items: List of dictionaries, each containing:
        - icon: Material icon name (without the 'material/' prefix)
        - title: Card title
        - description: Card description
    """
    # Add CSS for card styling
    st.markdown("""
    <style>
        .card-grid {
            display: flex;
            flex-wrap: wrap;
            gap: 16px;
            margin: 24px 0;
        }
        .feature-card {
            flex: 1;
            min-width: 200px;
            background-color: blue;
            border: 1px solid rgba(49, 51, 63, 0.2);
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        .feature-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 8px 15px rgba(0,0,0,0.1);
        }
        .card-title {
            font-weight: bold;
            font-size: 1.1rem;
            margin-bottom: 10px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .card-description {
            color: #666;
        }
    </style>
    """, unsafe_allow_html=True)

    # Create columns for the cards
    cols = st.columns(len(items))

    # Generate each card in the appropriate column
    for col, item in zip(cols, items):
        with col:
            # Each column gets its own card
            with st.container():
                # Show the icon and title
                if 'icon' in item.keys():
                    st.markdown(f"### **:material/{item['icon']}:**")

                st.markdown(f"##### **{item['title']}**")

                # Show the description
                st.markdown(item['description'])

                # Add spacing
                st.markdown("<br>", unsafe_allow_html=True)

                # Apply card styling to this container
                st.markdown("""
                <style>
                    div[data-testid="column"] > div:first-child {
                        background-color: white;
                        border: 1px solid rgba(49, 51, 63, 0.2);
                        border-radius: 8px;
                        padding: 16px;
                        height: 100%;
                        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                        transition: transform 0.3s ease, box-shadow 0.3s ease;
                    }
                    div[data-testid="column"] > div:first-child:hover {
                        transform: translateY(-4px);
                        box-shadow: 0 8px 15px rgba(0,0,0,0.1);
                    }
                    
                    div[data-testid="column"] h3 {
                        margin-top: 4px;
                        margin-bottom: 4px;
                        padding-top: 0;
                        padding-bottom: 0;
                    }
                </style>
                """, unsafe_allow_html=True)


def display_population_projections(county_name, state_name, county_fips, population_historical, population_projections):
    st.write(f"### Population Projections for {county_name}, {state_name}")

    county_pop_historical = population_historical.loc[county_fips]

    # If the county has multiple rows of data, select the row with the most complete data
    if county_pop_historical.shape[0] > 1:
        # Count the number of missing values in each row
        missing_counts = county_pop_historical.isna().sum(axis=1)

        # Get the index of the row with the minimum number of missing values
        min_missing_idx = missing_counts.idxmin()

        county_pop_historical = county_pop_historical.loc[min_missing_idx]

    county_pop_projections = population_projections.loc[county_fips]

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
        projected_pop_2065 = county_pop_projections[scenario]

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


def display_migration_impact_analysis(projections_dict, scenario):
    impact_map = {
        "Scenario S5b": "Low",
        "Scenario S5a": "Medium",
        "Scenario S5c": "High"
    }

    # Calculate metrics based on selected scenario vs baseline
    baseline_pop_2065 = projections_dict["POPULATION_2065_S3"]
    selected_pop_2065 = projections_dict[scenario]

    # Calculate additional residents (difference between selected scenario and baseline)
    additional_residents = int(selected_pop_2065 - baseline_pop_2065)

    # Calculate percentage increase relative to baseline
    percent_increase = round(
        (additional_residents / baseline_pop_2065) * 100, 1)

    # Display metrics in same row
    split_row(
        lambda: st.metric(
            label="Estimated Population by 2065",
            value=f"{selected_pop_2065:,}",
            delta=None if additional_residents == 0 else (
                f"{additional_residents:,.0f}" if additional_residents > 0 else f"{additional_residents:,.0f}")
        ),
        lambda: st.metric(
            label="Population Increase",
            value=f"{percent_increase}%",
        ),
        [0.5, 0.5]
    )


def display_housing_indicators(county_name, state_name, county_fips):
    st.header('Housing Analysis')

    st.write(f"### Median Gross Rent for {county_name}, {state_name}")
    st.line_chart(database.get_stat_var(Table.COUNTY_HOUSING_DATA,
                  "MEDIAN_GROSS_RENT", county_fips=county_fips))

    st.write(f"### Median House Value for {county_name}, {state_name}")
    st.line_chart(database.get_stat_var(Table.COUNTY_HOUSING_DATA,
                  "MEDIAN_HOUSING_VALUE", county_fips=county_fips))

    st.write(f"### Total Housing Units for {county_name}, {state_name}")
    st.line_chart(database.get_stat_var(Table.COUNTY_HOUSING_DATA,
                  "TOTAL_HOUSING_UNITS", county_fips=county_fips))

    st.write(f"### Occupied Housing Units for {county_name}, {state_name}")
    st.line_chart(database.get_stat_var(Table.COUNTY_HOUSING_DATA,
                  "OCCUPIED_HOUSING_UNITS", county_fips=county_fips))


def display_education_indicators(county_name, state_name, county_fips):
    st.header('Education Analysis')

    # Retrieve all the educational attainment data needed for the chart
    less_than_hs_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "LESS_THAN_HIGH_SCHOOL_TOTAL", county_fips=county_fips)
    hs_graduate_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "HIGH_SCHOOL_GRADUATE_TOTAL", county_fips=county_fips)
    some_college_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "SOME_COLLEGE_TOTAL", county_fips=county_fips)
    bachelors_higher_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "BACHELORS_OR_HIGHER_TOTAL", county_fips=county_fips)
    total_pop_25_64_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "TOTAL_POPULATION_25_64", county_fips=county_fips)

    # Combine all dataframes into one
    final_df = pd.DataFrame()
    final_df["YEAR"] = less_than_hs_df.index
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
        go.Scatter(x=final_df["YEAR"], y=final_df["LessThanHighSchool_Perc"],
                   mode="lines+markers", name="Less than High School (%)",
                   marker=dict(symbol="circle")),
        secondary_y=False
    )

    fig.add_trace(
        go.Scatter(x=final_df["YEAR"], y=final_df["HighSchoolGraduate_Perc"],
                   mode="lines+markers", name="High School Graduate (%)",
                   marker=dict(symbol="square")),
        secondary_y=False
    )

    fig.add_trace(
        go.Scatter(x=final_df["YEAR"], y=final_df["SomeCollege_Perc"],
                   mode="lines+markers", name="Some College or Associate's Degree (%)",
                   marker=dict(symbol="triangle-up")),
        secondary_y=False
    )

    fig.add_trace(
        go.Scatter(x=final_df["YEAR"], y=final_df["BachelorsOrHigher_Perc"],
                   mode="lines+markers", name="Bachelor's Degree or Higher (%)",
                   marker=dict(symbol="diamond")),
        secondary_y=False
    )

    # Add trace for total population (right y-axis)
    fig.add_trace(
        go.Scatter(x=final_df["YEAR"], y=final_df["TOTAL_POPULATION_25_64"],
                   mode="lines+markers", name="Total Population (25-64)",
                   line=dict(dash="dash", color="black"),
                   marker=dict(symbol="star", color="black")),
        secondary_y=True
    )

    # Set axis titles
    fig.update_xaxes(title_text="YEAR")
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


def display_unemployment_indicators(county_name, state_name, county_fips):
    st.header('Unemployment Analysis')

    # Retrieve the unemployment data needed for the chart
    # Using the same pattern as your education function but with economic data table
    total_labor_force_df = database.get_stat_var(
        Table.COUNTY_ECONOMIC_DATA, "TOTAL_LABOR_FORCE", county_fips=county_fips)
    unemployed_persons_df = database.get_stat_var(
        Table.COUNTY_ECONOMIC_DATA, "UNEMPLOYED_PERSONS", county_fips=county_fips)
    unemployment_rate_df = database.get_stat_var(
        Table.COUNTY_ECONOMIC_DATA, "UNEMPLOYMENT_RATE", county_fips=county_fips)

    # Combine all dataframes into one
    total_unemployment = pd.DataFrame()
    total_unemployment["YEAR"] = total_labor_force_df.index
    total_unemployment["TotalLaborForce"] = total_labor_force_df.values
    total_unemployment["TotalUnemployed"] = unemployed_persons_df.values
    total_unemployment["UnemploymentRate"] = unemployment_rate_df.values

    # Create a title for the chart
    st.write(
        f"###### Total Labor Force, Unemployed Population, and Unemployment Rate (2011-2023)")

    # Create a figure with secondary y-axis using Plotly
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add trace for Total Labor Force (left y-axis)
    fig.add_trace(
        go.Scatter(x=total_unemployment["YEAR"], y=total_unemployment["TotalLaborForce"],
                   mode="lines+markers", name="Total Labor Force",
                   line=dict(color="blue"),
                   marker=dict(symbol="circle", color="blue")),
        secondary_y=False
    )

    # Add trace for Total Unemployed (left y-axis)
    fig.add_trace(
        go.Scatter(x=total_unemployment["YEAR"], y=total_unemployment["TotalUnemployed"],
                   mode="lines+markers", name="Total Unemployed",
                   line=dict(color="red"),
                   marker=dict(symbol="square", color="red")),
        secondary_y=False
    )

    # Add trace for Unemployment Rate (right y-axis)
    fig.add_trace(
        go.Scatter(x=total_unemployment["YEAR"], y=total_unemployment["UnemploymentRate"],
                   mode="lines+markers", name="Unemployment Rate (%)",
                   line=dict(dash="dash", color="green"),
                   marker=dict(symbol="triangle-up", color="green")),
        secondary_y=True
    )

    # Set axis titles
    fig.update_xaxes(title_text="YEAR")
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


def display_unemployment_by_education(county_name, state_name, county_fips):
    st.header('Unemployment by Education Level')

    # Retrieve raw counts for each education level - both unemployed and total population
    # Unemployed counts
    less_than_hs_unemployed_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "LESS_THAN_HIGH_SCHOOL_UNEMPLOYED", county_fips=county_fips)
    hs_graduate_unemployed_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "HIGH_SCHOOL_GRADUATE_UNEMPLOYED", county_fips=county_fips)
    some_college_unemployed_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "SOME_COLLEGE_UNEMPLOYED", county_fips=county_fips)
    bachelors_higher_unemployed_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "BACHELORS_OR_HIGHER_UNEMPLOYED", county_fips=county_fips)

    # Total population counts
    less_than_hs_total_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "LESS_THAN_HIGH_SCHOOL_TOTAL", county_fips=county_fips)
    hs_graduate_total_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "HIGH_SCHOOL_GRADUATE_TOTAL", county_fips=county_fips)
    some_college_total_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "SOME_COLLEGE_TOTAL", county_fips=county_fips)
    bachelors_higher_total_df = database.get_stat_var(
        Table.COUNTY_EDUCATION_DATA, "BACHELORS_OR_HIGHER_TOTAL", county_fips=county_fips)

    # Combine all dataframes into one
    unemployment_by_edulevel = pd.DataFrame()
    unemployment_by_edulevel["YEAR"] = less_than_hs_unemployed_df.index

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
        f"###### Unemployment Rate by Education Level (2011-2023)")

    # Create a figure using Plotly
    fig = go.Figure()

    # Add traces for each education level's unemployment rate
    fig.add_trace(
        go.Scatter(x=unemployment_by_edulevel["YEAR"],
                   y=unemployment_by_edulevel["LessThanHighSchool_UnemploymentRate"],
                   mode="lines+markers",
                   name="Less Than High School",
                   marker=dict(symbol="circle"))
    )

    fig.add_trace(
        go.Scatter(x=unemployment_by_edulevel["YEAR"],
                   y=unemployment_by_edulevel["HighSchoolGraduate_UnemploymentRate"],
                   mode="lines+markers",
                   name="High School Graduate",
                   marker=dict(symbol="square"))
    )

    fig.add_trace(
        go.Scatter(x=unemployment_by_edulevel["YEAR"],
                   y=unemployment_by_edulevel["SomeCollege_UnemploymentRate"],
                   mode="lines+markers",
                   name="Some College or Associate's Degree",
                   marker=dict(symbol="triangle-up"))
    )

    fig.add_trace(
        go.Scatter(x=unemployment_by_edulevel["YEAR"],
                   y=unemployment_by_edulevel["BachelorsOrHigher_UnemploymentRate"],
                   mode="lines+markers",
                   name="Bachelor's Degree or Higher",
                   marker=dict(symbol="diamond"))
    )

    # Set axis titles and layout
    fig.update_xaxes(title_text="YEAR")
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

st.html(
    '<h1 class="custom-title">Is America Ready to Move?</h1>'
)

# Initialize the Database connection
db_conn = get_db_connection()

# Make all database calls using database instead of just db
counties = database.get_county_metadata().set_index('COUNTY_FIPS')

population_historical = database.get_population_timeseries().set_index('COUNTY_FIPS')

population_projections = database.get_population_projections_by_fips(
).set_index('COUNTY_FIPS')

selected_county_fips = '36029'

with st.sidebar:
    selected_county_fips = st.selectbox(
        'Select a county',
        counties.index,
        format_func=lambda fips: counties['NAME'].loc[fips],
        placeholder='Type to search...',
        index=counties.index.get_loc(selected_county_fips)
    )

    impact_map = {
        # "POPULATION_2065_S3": "Baseline",
        "POPULATION_2065_S5a": "Low",
        "POPULATION_2065_S5b": "Medium",
        "POPULATION_2065_S5c": "High"
    }

    selected_scenario = st.selectbox(
        "Select a climate impact scenario:",
        # Exclude Scenario S3 (baseline)
        options=list(impact_map.keys()),
        format_func=lambda sel: impact_map.get(sel),
        index=0
    )

    display_migration_impact_analysis(
        population_projections.loc[selected_county_fips],
        selected_scenario
    )
    
    vertical_spacer(5)

    national_risk_score(selected_county_fips)


# Short paragraph explaining why climate migration will occur and how
st.markdown("""
# Climate-Induced Migration
Climate change is increasingly driving population shifts across the United States. As extreme weather events become more frequent and severe, communities around the country face challenges including sea-level rise, extreme heat, drought, wildfires, and flooding. These environmental pressures are expected to force increasingly more people to relocate from high-risk areas to regions with better climate resilience, impacting local economies, housing markets, and public services.
""")

# Climate migration choropleth of US counties
fema_nri_map(selected_scenario)

st.markdown("""
            ### Climate Vulnerability Isn't the Whole Story
            """)
st.markdown("""
            Of course, climate vulnerability won't be the only factor that drives migration decisions. While some people may consider leaving areas prone to climate hazards, research shows that economic factors like job opportunities and wages will still play a dominant role in determining if, when, and where people relocate.
            """)

feature_cards(
    [
        {"icon": "house", "title": "Housing Cost",
            "description": "Availability of affordable housing"},
        {"icon": "work", "title": "Labor Demand",
            "description": "Strength of local job markets"},
        {"icon": "cloud_alert", "title": "Climate Risks",
            "description": "Vulnerability to climate hazards"},
    ]
)


# Explain factors that will affect the magnitude of climate-induced migration

with st.expander("Read more about migration factors", icon=":material/article:"):
    st.markdown("""When regions experiencing population loss due to climate concerns face labor shortages, wages tend to rise, creating an economic incentive for some people to stay or even move into these areas despite climate risks. Housing prices also adjust, becoming more affordable in areas experiencing outmigration, which further complicates migration patterns. This economic "dampening effect" means that even highly climate-vulnerable counties won't see mass exoduses, as financial considerations, family ties, and community connections often outweigh climate concerns in people's decision-making process. Migration is ultimately a complex interplay of climate, economic, social, and personal factors rather than a simple response to climate vulnerability alone. The key migration decision factors included in this model are:""")

vertical_spacer(5)

# Get the County FIPS code, which will be used for all future queries
if selected_county_fips:
    county_metadata = database.get_county_metadata().iloc[0]
    # Separate the county and state names
    full_name = county_metadata['NAME']
    county_name, state_name = full_name.split(', ')
else:
    county_name = state_name = selected_county_fips = None

if selected_county_fips:
    
    
    population_by_climate_region(selected_scenario)

    st.markdown("""
                These projections help visualize how climate change could reshape population distribution across regions, with some areas experiencing population growth (Northeast, West, California) and others facing decline (South, Midwest) due to climate-related migration pressures.

                The data is derived from research on climate-induced migration patterns, which considers factors including extreme weather events, economic opportunities, and regional climate vulnerabilities.
                """)

    # 7. Show socioeconomic indicator analyses
    # st.markdown("### Socioeconomic Indicator Projections")
    # st.markdown(
    #     f"The following indicators show how {county_name} may be affected by projected population changes:")

    # st.write("##### Index projections")
    # projected_indices_df = database.get_table_for_county(Table.COUNTY_PROJECTED_INDICES, selected_county_fips)
    # st.write(projected_indices_df)
    
    # st.write("##### Socioeconomic indices")
    # socioeconomic_indices_df = database.get_table_for_county(Table.COUNTY_SOCIOECONOMIC_INDEX_DATA, selected_county_fips)
    # st.write(socioeconomic_indices_df)
    
    # plot_socioeconomic_indices(socioeconomic_indices_df)
    # plot_socioeconomic_radar(socioeconomic_indices_df)
    
    
    
    ######################################################################
    ######################################################################
    ######################################################################
    ######################################################################
    ######################################################################
    projected_data = database.get_table_for_county(Table.COUNTY_COMBINED_PROJECTIONS, selected_county_fips)
    # st.write(projected_data)
    
    # Display the impact analysis
    display_scenario_impact_analysis(county_name, state_name, projected_data)
    
    # Display policy recommendations
    generate_policy_recommendations(projected_data)
    


    
    # if not population_projections.empty:
    #     display_population_projections(
    #         county_name, state_name, selected_county_fips, population_historical, population_projections)


    # display_education_indicators(
        # county_name, state_name, selected_county_fips)

    # split_row(
    #     lambda: display_unemployment_indicators(
    #         county_name, state_name, selected_county_fips),
    #     lambda: display_unemployment_by_education(
    #         county_name, state_name, selected_county_fips),
    #     [0.5, 0.5]
    # )
    ######################################################################
    ######################################################################
    ######################################################################
    ######################################################################