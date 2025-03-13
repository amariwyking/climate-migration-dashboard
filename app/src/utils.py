import streamlit as st
import pandas as pd

def get_all_county_names():
    # import table containing county names and FIPS
    counties = pd.read_csv('../data/county_names.csv')

    return counties.COUNTY_NAME

def get_county_fips_code(county_name: str) -> str:
    # import table containing county names and FIPS
    counties = pd.read_csv('../data/county_names.csv')
    
    search_results = counties[counties.COUNTY_NAME == county_name]['COUNTY_FIPS']
    
    # If there are no search results return None
    if search_results.empty:
        return None

    # Get the first search result and format as a 5-digit code
    fips_code = str(search_results.iloc[0]).zfill(5)
    
    return fips_code
    
    
def add_custom_css():
    st.markdown(
        """
        <style>
        </style>
        """,
        unsafe_allow_html=True
    )