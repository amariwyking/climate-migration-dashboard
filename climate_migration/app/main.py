import streamlit as st

from src.utils import add_custom_css

st.set_page_config(
    layout="wide",
    initial_sidebar_state='collapsed',
)
add_custom_css()


def main():
    about_page = st.Page("src/pages/page1.py", title="About this project")
    dashboard_page = st.Page("src/pages/dashboard.py", title="Dashboard")

    pg = st.navigation([dashboard_page, about_page])
    pg.run()


if __name__ == "__main__":
    main()
