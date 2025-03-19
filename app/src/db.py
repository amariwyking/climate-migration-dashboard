import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file


def get_db_connection():
    """Create and return a PostgreSQL database connection"""
    try:
        engine = create_engine(os.getenv("DATABASE_URL"))
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