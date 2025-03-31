import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from enum import Enum

class Table(Enum):
    # Housing related tables
    MEDIAN_GROSS_RENT = "timeseries_median_gross_rent"
    MEDIAN_HOUSE_VALUE = "timeseries_median_house_value"
    OCCUPIED_HOUSING_UNITS = "timeseries_occupied_housing_units"
    TOTAL_HOUSING_UNITS = "timeseries_total_housing_units"
    
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
        
        # Set COUNTY_FIPS as the index
        # if not df.empty:
            # df.set_index('COUNTY_FIPS', inplace=True)
            
            # print(df)
            
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
        st.error(f"Error loading historical population counts: {str(e)}")
        st.stop()

def get_county_data_table(conn, table_name, county_fips=None):
    """
    Get time series data for median gross rent for the specified county by FIPS code
    
    Parameters:
    -----------
    conn : database connection
        PostgreSQL database connection
    table_name : SQL table name
        Name of the table to query in the database.
    county_fips : int or list, optional
        County FIPS code(s) to query. If None, returns all counties.
    
    Returns:
    --------
    df : pandas.DataFrame
        DataFrame containing population projection data
    """
    try:
        query = f"SELECT * FROM {table_name.value}"
        
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
        st.error(f"Error loading historical population counts: {str(e)}")
        st.stop()
        
def get_county_metadata(conn, county_fips=None):
    """
    Get county time series data from the specified table
    
    Parameters:
    -----------
    conn : database connection
        PostgreSQL database connection
    table_name : SQL table name
        Name of the table to query in the database.
    county_fips : int or list, optional
        County FIPS code(s) to query. If None, returns all counties.
    
    Returns:
    --------
    df : pandas.DataFrame
        DataFrame containing population projection data
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
