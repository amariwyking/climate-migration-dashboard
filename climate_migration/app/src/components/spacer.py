import streamlit as st

def vertical_spacer(n=1):
    """
    Adds vertical space to a Streamlit app using HTML line breaks.
    
    Parameters:
    -----------
    n : int
        Number of line breaks to add (default: 1)
    """
    # Ensure n is a positive integer
    n = max(1, int(n))
    
    # Create n number of <br> tags
    breaks = "<br>" * n
    
    # Use markdown to render the breaks
    st.markdown(breaks, unsafe_allow_html=True)