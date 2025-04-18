import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from enum import Enum


class Table(Enum):
    # County table
    COUNTY_METADATA = "county"

    COUNTY_HOUSING_DATA = "cleaned_housing_data"
    COUNTY_ECONOMIC_DATA = "cleaned_economic_data"
    COUNTY_EDUCATION_DATA = "cleaned_education_data"
    COUNTY_CRIME_DATA = "cleaned_crime_data"
    COUNTY_FEMA_DATA = "cleaned_fema_nri_data"
    COUNTY_JOB_OPENING_DATA = "cleaned_job_openings_data"
    COUNTY_SOCIOECONOMIC_INDEX_DATA = "socioeconomic_indices"
    COUNTY_SOCIOECONOMIC_RANKING_DATA = "socioeconomic_indices_rankings"

    # Population related tables
    POPULATION_HISTORY = "timeseries_population"
    POPULATION_PROJECTIONS = "county_population_projections"


# Load environment-specific .env file
ENVIRONMENT = os.getenv(
    "ENVIRONMENT", "dev"
)  # Default to dev, change to prod when deploying
env_file = f".env.{ENVIRONMENT}" if ENVIRONMENT != "dev" else ".env"
load_dotenv(env_file)

# Fix Heroku connection string
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL is None:
    raise ValueError("DATABASE_URL not found in environment variables.")

# Set SSL mode based on environment
SSL_MODE = "require" if ENVIRONMENT == "prod" else "disable"


def get_db_connection():
    """Create and return a PostgreSQL database connection"""
    try:
        engine = create_engine(
            DATABASE_URL.replace("postgres://", "postgresql://", 1),
            connect_args={"sslmode": SSL_MODE},
        )
        
        conn = engine.connect()

        print(f"Dashboard running from \033[1m{ENVIRONMENT}\033[0m environment")
        print(f"Database connection established via URL: \033[1m{conn.engine.url}\033[0m")
        print()

        return conn
    except Exception as e:
        raise Exception(f"Database connection failed: {str(e)}")


def get_population_projections_by_fips(conn, county_fips=None):
    """
    Get population projections for a specific county by FIPS code

    Parameters:
    -----------
    conn : database connection
        PostgreSQL database connection
    county_fips : int or list, optional
        County FIPS code(s) to query. If None, returns all counties.

    Returns:
    --------
    df : pandas.DataFrame
        DataFrame containing population projection data
    """
    try:
        query = "SELECT * FROM county_population_projections"

        # Add COUNTY_FIPS filter if provided
        if county_fips is not None:
            if isinstance(county_fips, list):
                fips_list = ", ".join(str(fips) for fips in county_fips)
                query += f" WHERE COUNTY_FIPS IN ({fips_list})"
            else:
                query += f" WHERE COUNTY_FIPS = {county_fips}"

        # Execute query and return as DataFrame
        df = pd.read_sql(query, conn)

        return df
    except Exception as e:
        st.error(f"Error loading population projections: {str(e)}")
        st.stop()


def get_population_timeseries(conn, county_fips=None):
    """
    Get population history for a specific county by FIPS code

    Parameters:
    -----------
    conn : database connection
        PostgreSQL database connection
    county_fips : int or list, optional
        County FIPS code(s) to query. If None, returns all counties.

    Returns:
    --------
    df : pandas.DataFrame
        DataFrame containing population projection data
    """
    try:
        query = "SELECT * FROM timeseries_population"

        # Add COUNTY_FIPS filter if provided
        if county_fips is not None:
            if isinstance(county_fips, list):
                fips_list = ", ".join(str(fips) for fips in county_fips)
                query += f" WHERE COUNTY_FIPS IN ({fips_list})"
            else:
                query += f" WHERE COUNTY_FIPS = {county_fips}"

        # Execute query and return as DataFrame
        df = pd.read_sql(query, conn)

        return df
    except Exception as e:
        st.error(f"Error loading historical population counts: {str(e)}")
        st.stop()


def get_timeseries_median_gross_rent(conn, county_fips=None):
    """
    Get time series data for median gross rent for the specified county by FIPS code

    Parameters:
    -----------
    conn : database connection
        PostgreSQL database connection
    county_fips : int or list, optional
        County FIPS code(s) to query. If None, returns all counties.

    Returns:
    --------
    df : pandas.DataFrame
        DataFrame containing population projection data
    """
    try:
        query = "SELECT * FROM timeseries_median_gross_rent"

        # Add COUNTY_FIPS filter if provided
        if county_fips is not None:
            if isinstance(county_fips, list):
                fips_list = ", ".join(str(fips) for fips in county_fips)
                query += f" WHERE \"COUNTY_FIPS\" IN ({fips_list})"
            else:
                query += f" WHERE \"COUNTY_FIPS\" = {county_fips}"

        # Execute query and return as DataFrame
        df = pd.read_sql(query, conn).set_index("COUNTY_FIPS")

        return df.T
    except Exception as e:
        st.error(f"Error loading historical median gross rent: {str(e)}")
        st.stop()


def get_stat_var(conn, table: Table, indicator_name, county_fips, year: int = None):
    """
    Get county data from a statistical variable's specified table

    Parameters:
    -----------
    conn : database connection
        PostgreSQL database connection
    table : SQL table to be queried
        Enum for the table to query in the database.
    indicator_name : Indicator name
        Name of the indicator to pull from the table.
    county_fips : int or list, optional
        County FIPS code(s) to query. If None, returns all counties.

    Returns:
    --------
    df : pandas.DataFrame
        DataFrame containing population projection data
    """
    table_name = table.value

    try:
        # Create base query
        query = f'SELECT "Year", "{indicator_name}" FROM "{table_name}"'

        # Initialize parameters dictionary
        params = {}

        # Add COUNTY_FIPS filter if provided
        if county_fips is not None:
            if isinstance(county_fips, list):
                # For multiple counties
                query += " WHERE \"COUNTY_FIPS\" IN :county_fips"
                params['county_fips'] = tuple(
                    str(fips) for fips in county_fips)
            else:
                # For single county
                query += " WHERE \"COUNTY_FIPS\" = :county_fips"
                params['county_fips'] = str(county_fips)
                
            if year:
                query += f' AND "Year" = :year'
                params['year'] = year

        # Sort the results of the query
        query += f" ORDER BY \"{table_name}\".\"Year\" ASC"

        # Convert to SQLAlchemy text object
        sql_query = text(query)

        # Execute query and return as DataFrame
        df = pd.read_sql(sql_query, conn, params=params)

        df.Year = pd.to_datetime(df.Year, format='%Y').dt.year
        df = df.set_index("Year")

        return df
    except Exception as e:
        st.error(f"Error loading time series data: {str(e)}")
        st.stop()


def get_county_metadata(conn, county_fips=None):
    """
    Get county time series data from the specified table

    Parameters:
    -----------
    conn : database connection
        PostgreSQL database connection
    county_fips : int or list, optional
        County FIPS code(s) to query. If None, returns all counties.

    Returns:
    --------
    df : pandas.DataFrame
        DataFrame containing county metadata
    """
    try:
        query = f"SELECT * FROM {Table.COUNTY_METADATA.value}"

        # Add COUNTY_FIPS filter if provided
        if county_fips is not None:
            if isinstance(county_fips, list):
                fips_list = ", ".join(str(fips) for fips in county_fips)
                query += f" WHERE \"COUNTY_FIPS\" IN ({fips_list})"
            else:
                query += f" WHERE \"COUNTY_FIPS\" = {county_fips}"

        # Execute query and return as DataFrame
        df = pd.read_sql(query, conn).set_index("COUNTY_FIPS")

        return df
    except Exception as e:
        st.error(f"Error loading county data counts: {str(e)}")
        st.stop()
