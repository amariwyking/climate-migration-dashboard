import streamlit as st
import pandas as pd

from src.utils import get_all_county_names, get_county_fips_code

# TODO: Can we package the county name and FIPS code in the selectbox?
county_name = st.selectbox(
    'Select a county',
    get_all_county_names(),
    placeholder='Type to search...',
    index=None
)

# Get the County FIPS code, which will be used for all future queries
if county_name:
    county_fips = get_county_fips_code(county_name)
else:
    county_fips = None