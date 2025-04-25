import pathlib
import streamlit as st

from src.utils import add_custom_css

st.set_page_config(
    layout="wide",
    initial_sidebar_state='expanded',
)

css_path = pathlib.Path("app/assets/styles.css")
add_custom_css(css_path)

def main():
    about_page = st.Page("src/pages/page1.py", title="About this project")
    dashboard_page = st.Page("src/pages/dashboard.py", title="Dashboard")

    pg = st.navigation([dashboard_page, about_page])
    pg.run()


if __name__ == "__main__":
    main()
