import streamlit as st

from src.utils import add_custom_css
from src.pages import PAGE_MAP

add_custom_css()

def main(state=None):
    current_page = st.sidebar.radio("Go To", list(PAGE_MAP))
    PAGE_MAP[current_page]().write()

if __name__ == "__main__":
    main()