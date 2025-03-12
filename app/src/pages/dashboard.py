import streamlit as st
import pandas as pd

from src.utils import get_county_names

st.selectbox(
    'Select a county',
    get_county_names(),
    placeholder='Type to search...',
    index=None
)
