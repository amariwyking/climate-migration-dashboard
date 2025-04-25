import streamlit as st
import pandas as pd

from typing import List

DATA_DIR = "../data/"

def get_all_county_names():
    # import table containing county names and FIPS
    counties = pd.read_csv(DATA_DIR + "raw/county_names.csv")

    return counties.COUNTY_NAME

def get_all_county_fips():
    # import table containing county names and FIPS
    counties = pd.read_csv(DATA_DIR + "raw/county_names.csv")

    return counties.COUNTY_FIPS

def get_county_fips_code(county_name: str) -> str:
    # import table containing county names and FIPS
    counties = pd.read_csv(DATA_DIR + "raw/county_names.csv")
    
    search_results = counties[counties.COUNTY_NAME == county_name]["COUNTY_FIPS"]
    
    # If there are no search results return None
    if search_results.empty:
        return None

    # Get the first search result and format as a 5-digit code
    fips_code = str(search_results.iloc[0]).zfill(5)
    
    return fips_code

def get_county_population_history(county_fips: str) -> pd.DataFrame:
    if not county_fips:
        return None
    
    print('Searching for county with FIPS code: ' + county_fips)
    
    df_population = pd.read_csv(DATA_DIR + "decennial_county_population_data_1900_1990.csv", dtype=str)
    df_population = df_population.set_index('fips').drop(columns=['name'])
    
    print(df_population)
    
    return df_population.loc[county_fips]

    
    
def add_custom_css(file_path):
    with open(file_path) as f:
        st.html(f"<style>{f.read()}</style>")