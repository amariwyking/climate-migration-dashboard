import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from enum import Enum
from typing import Optional, List, Union


class Table(Enum):
    # County table
    COUNTY_METADATA = "county"

    COUNTY_HOUSING_DATA = "cleaned_housing_data"
    COUNTY_ECONOMIC_DATA = "cleaned_economic_data"
    COUNTY_EDUCATION_DATA = "cleaned_education_data"
    COUNTY_CRIME_DATA = "cleaned_crime_data"
    COUNTY_FEMA_DATA = "cleaned_fema_nri_data"
    COUNTY_CBSA_DATA = "cleaned_cbsa_data"
    COUNTY_JOB_OPENING_DATA = "cleaned_job_openings_data"
    COUNTY_SOCIOECONOMIC_INDEX_DATA = "socioeconomic_indices"
    COUNTY_SOCIOECONOMIC_RANKING_DATA = "socioeconomic_indices_rankings"
    COUNTY_PROJECTED_INDICES = "projected_socioeconomic_indices"
    
    COUNTY_COMBINED_PROJECTIONS = "combined_2065_data"

    # Population related tables
    POPULATION_HISTORY = "timeseries_population"
    POPULATION_PROJECTIONS = "county_population_projections"


class Database:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # Load environment-specific .env file
        # Default to dev, change to prod when deploying
        ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
        env_file = f".env.{ENVIRONMENT}" if ENVIRONMENT != "dev" else ".env"
        load_dotenv(env_file)

        # Fix Heroku connection string
        self.database_url = os.getenv("DATABASE_URL")

        if self.database_url is None:
            raise ValueError(
                "DATABASE_URL not found in environment variables.")

        # Set SSL mode based on environment
        self.ssl_mode = "require" if ENVIRONMENT == "prod" else "disable"
        self.environment = ENVIRONMENT
        self.conn = None
        self._initialized = True

    def connect(self):
        """Create and return a PostgreSQL database connection"""
        if self.conn is not None:
            return self.conn

        try:
            engine = create_engine(
                self.database_url.replace("postgres://", "postgresql://", 1),
                connect_args={"sslmode": self.ssl_mode},
            )

            self.conn = engine.connect()

            print(
                f"Dashboard running from \033[1m{self.environment}\033[0m environment")
            print(
                f"Database connection established via URL: \033[1m{self.conn.engine.url}\033[0m")
            print()

            return self.conn
        except Exception as e:
            raise Exception(f"Database connection failed: {str(e)}")

    def close(self):
        """Close the database connection"""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    @st.cache_data
    def get_population_projections_by_fips(_self, county_fips: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
        """
        Get population projections for a specific county by FIPS code

        Parameters:
        -----------
        county_fips : str or list, optional
            County FIPS code(s) to query. If None, returns all counties.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing population projection data
        """
        conn = _self.conn
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

    @st.cache_data
    def get_population_timeseries(_self, county_fips: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
        """
        Get population history for a specific county by FIPS code

        Parameters:
        -----------
        county_fips : str or list, optional
            County FIPS code(s) to query. If None, returns all counties.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing population projection data
        """
        conn = _self.conn
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

    @st.cache_data
    def get_timeseries_median_gross_rent(_self, county_fips: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
        """
        Get time series data for median gross rent for the specified county by FIPS code

        Parameters:
        -----------
        county_fips : str or list, optional
            County FIPS code(s) to query. If None, returns all counties.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing population projection data
        """
        conn = _self.conn
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

    @st.cache_data
    def get_stat_var(_self, table: Table, indicator_name: str, county_fips: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        Get county data from a statistical variable's specified table

        Parameters:
        -----------
        table : SQL table to be queried
            Enum for the table to query in the database.
        indicator_name : str
            Name of the indicator to pull from the table.
        county_fips : str or list
            County FIPS code(s) to query. If None, returns all counties.
        year : int, optional
            Specific year to query. If None, returns all years.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing population projection data
        """
        conn = _self.conn
        table_name = table.value

        try:
            # Initialize parameters dictionary
            params = {}

            # Add COUNTY_FIPS filter if provided
            if county_fips is not None:
                if isinstance(county_fips, list):
                    # Create base query
                    query = f'SELECT "YEAR", "{indicator_name}", "COUNTY_FIPS" FROM "{table_name}"'

                    # For multiple counties
                    query += " WHERE \"COUNTY_FIPS\" IN :county_fips"
                    params['county_fips'] = tuple(
                        str(fips) for fips in county_fips)
                else:
                    # Create base query
                    query = f'SELECT "YEAR", "{indicator_name}" FROM "{table_name}"'

                    # For single county
                    query += " WHERE \"COUNTY_FIPS\" = :county_fips"
                    params['county_fips'] = str(county_fips)

                if year:
                    query += f' AND "YEAR" = :year'
                    params['year'] = year

            # Sort the results of the query
            query += f" ORDER BY \"{table_name}\".\"YEAR\" ASC"

            # Convert to SQLAlchemy text object
            sql_query = text(query)

            # Execute query and return as DataFrame
            df = pd.read_sql(sql_query, conn, params=params)

            df.YEAR = pd.to_datetime(df.YEAR, format='%Y').dt.year
            df = df.set_index("YEAR")

            return df
        except Exception as e:
            st.error(f"Error loading time series data: {str(e)}")
            st.stop()

    @st.cache_data
    def get_county_metadata(_self, county_fips: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
        """
        Get county time series data from the specified table

        Parameters:
        -----------
        county_fips : str or list, optional
            County FIPS code(s) to query. If None, returns all counties.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing county metadata
        """
        conn = _self.conn
        try:
            # Start with the base query
            query = f"SELECT * FROM {Table.COUNTY_METADATA.value}"

            # Add COUNTY_FIPS filter if provided
            if county_fips is not None:
                if isinstance(county_fips, list):
                    # Create proper parameter placeholders for IN clause
                    placeholders = ", ".join(
                        f":fips_{i}" for i in range(len(county_fips)))
                    query += f" WHERE \"COUNTY_FIPS\" IN ({placeholders})"

                    # Create a dictionary of parameters
                    params = {f"fips_{i}": fips for i,
                              fips in enumerate(county_fips)}
                else:
                    query += " WHERE \"COUNTY_FIPS\" = :county_fips"
                    params = {'county_fips': county_fips}
            else:
                params = {}

            # Convert to SQLAlchemy text object
            sql_query = text(query)

            # Execute query and return as DataFrame
            df = pd.read_sql(sql_query, conn, params=params)

            return df
        except Exception as e:
            st.error(f"Error loading county data counts: {str(e)}")
            st.stop()

    @st.cache_data
    def get_cbsa_counties(_self, filter: Optional[str] = None) -> pd.DataFrame:
        """
        Get counties that belong to a metropolitan statistical area (MSA)

        Parameters:
        -----------
        filter : str, optional
            Type of CBSA to filter for.
            Valid values: 'metro', 'micro', or None (returns all)
            Default is None.

        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing the counties along with MSA data
        """
        conn = _self.conn
        try:
            query = f'SELECT "COUNTY_FIPS", "CBSA", "TYPE" FROM {Table.COUNTY_CBSA_DATA.value}'

            if filter is not None and isinstance(filter, str):
                if filter == 'metro':
                    query += f" WHERE \"TYPE\" = 'Metropolitan Statistical Area'"
                elif filter == 'micro':
                    query += f" WHERE \"TYPE\" = 'Micropolitan Statistical Area'"

            # Execute query and return as DataFrame
            df = pd.read_sql(query, conn)

            return df
        except Exception as e:
            st.error(f"Error loading county CBSA data: {str(e)}")
            st.stop()

    @st.cache_data
    def get_projections_by_county(_self, county_fips: str) -> pd.DataFrame:
        """
        Get socioeconomic indices for a specific county by FIPS code
        
        Parameters:
        -----------
        county_fips : str
            County FIPS code to query.
            
        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing socioeconomic indices for the specified county
        """
        conn = _self.conn
        try:
            query = text("SELECT * FROM projected_socioeconomic_indices WHERE \"COUNTY_FIPS\" = :county_fips")
            
            # Execute query with parameter
            df = pd.read_sql(query, conn, params={'county_fips': county_fips})
            
            # Reset index and drop the old index
            df = df.reset_index(drop=True)
            
            return df
        except Exception as e:
            st.error(f"Error loading socioeconomic indices: {str(e)}")
            st.stop()
            
    @st.cache_data
    def get_table_for_county(_self, table: Table, county_fips: str) -> pd.DataFrame:
        """
        Get socioeconomic indices for a specific county by FIPS code
        
        Parameters:
        -----------
        county_fips : str
            County FIPS code to query.
            
        Returns:
        --------
        df : pandas.DataFrame
            DataFrame containing socioeconomic indices for the specified county
        """
        conn = _self.conn
        try:
            query = text(f"SELECT * FROM {table.value} WHERE \"COUNTY_FIPS\" = :county_fips")
            
            # Execute query with parameter
            df = pd.read_sql(query, conn, params={'county_fips': county_fips})
            
            # Reset index and drop the old index
            df = df.reset_index(drop=True)
            
            return df
        except Exception as e:
            st.error(f"Error loading socioeconomic indices: {str(e)}")
            st.stop()

# Create a singleton instance for easy import
db = Database()

# For backwards compatibility


def get_db_connection():
    """
    For backwards compatibility - returns the database connection
    """
    return db.connect()
