import streamlit as st

from src.utils import add_custom_css


add_custom_css()

def main():
    about_page = st.Page("src/pages/page1.py", title="About this project")
    
    pg = st.navigation([about_page])
    pg.run()

if __name__ == "__main__":
    main()