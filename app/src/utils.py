import streamlit as st
import pandas as pd

def get_county_names():
    counties = pd.read_csv('../data/county_names.csv')

    return counties.COUNTY_NAME

def add_custom_css():
    st.markdown(
        """
        <style>
        </style>
        """,
        unsafe_allow_html=True
    )