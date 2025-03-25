import sys
import os

# Get the absolute path of the parent directory of main.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
import pandas as pd
import plotly.express as px
from scripts.helpers import get_db_connection

st.set_page_config(page_title="Housing Market Trends", layout="wide", page_icon="🏠")


def load_housing_data():
    """Load housing data from PostgreSQL with caching"""
    try:
        with get_db_connection() as conn:
            query = """
            SELECT 
                "TOTAL_HOUSING_UNITS",
                "OCCUPIED_HOUSING_UNTIS",
                "MEDIAN_HOUSING_VALUE",
                "MEDIAN_GROSS_RENT",
                "COUNTY_FIPS",
                "Year",
                "POPULATION"
            FROM cleaned_housing_data
            ORDER BY "Year", "COUNTY_FIPS"
            """
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.stop()


def main():
    st.title("🏠 County Housing Market Analysis")

    # Load data with caching
    df = load_housing_data()

    # Convert FIPS to string for categorical treatment
    df["COUNTY_FIPS"] = df["COUNTY_FIPS"].astype(str)

    # Create sidebar filters
    st.sidebar.header("Filter Options")

    # Year range selector
    min_year, max_year = int(df["Year"].min()), int(df["Year"].max())
    selected_years = st.sidebar.slider(
        "Select Year Range",
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year),
    )

    # Metric selector
    metrics = {
        "Population": "POPULATION",
        "Total Housing Units": "TOTAL_HOUSING_UNITS",
        "Occupied Housing Units": "OCCUPIED_HOUSING_UNTIS",
        "Median Home Value": "MEDIAN_HOUSING_VALUE",
        "Median Rent": "MEDIAN_HOUSING_VALUE",
    }
    selected_metric = st.sidebar.selectbox(
        "Choose Metric to Analyze", list(metrics.keys()), index=0
    )

    # Filter data based on selections
    filtered_df = df[
        (df["Year"] >= selected_years[0]) & (df["Year"] <= selected_years[1])
    ]

    # Main content area
    col1, col2 = st.columns([3, 1])

    with col1:
        st.header(f"{selected_metric} Trends")

        # Create interactive time series plot
        fig = px.line(
            filtered_df,
            x="Year",
            y=metrics[selected_metric],
            color="COUNTY_FIPS",
            labels={
                "Year": "Year",
                metrics[selected_metric]: selected_metric,
                "COUNTY_FIPS": "County FIPS",
            },
            height=600,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.header("Quick Statistics")

        # Top counties analysis
        st.subheader(f"Top 5 Counties by {selected_metric} (Latest Year)")
        latest_year_data = filtered_df[filtered_df["Year"] == max_year]
        top_counties = latest_year_data.nlargest(5, metrics[selected_metric])
        top_counties["COUNTY_FIPS"] = top_counties["COUNTY_FIPS"].str.zfill(5)
        st.dataframe(
            top_counties[["COUNTY_FIPS", "Year", metrics[selected_metric]]].set_index(
                "COUNTY_FIPS"
            )
        )

        # Metric comparison
        st.subheader("Metric Comparison")
        metric_stats = filtered_df[metrics[selected_metric]].describe()
        st.write(f"**Average**: ${metric_stats['mean']:,.0f}")
        st.write(f"**Maximum**: ${metric_stats['max']:,.0f}")
        st.write(f"**Minimum**: ${metric_stats['min']:,.0f}")

    # Raw data section
    st.header("Raw Data Preview")
    st.dataframe(
        filtered_df.sort_values(["COUNTY_FIPS", "Year"]),
        height=300,
        column_config={
            "median_housing_value": st.column_config.NumberColumn(format="$%d"),
            "median_gross_rent": st.column_config.NumberColumn(format="$%d"),
        },
    )


if __name__ == "__main__":
    main()
