import streamlit as st
import pandas as pd

# Streamlit connection (secrets.toml)
conn = st.connection("mysql", type="sql")

st.title("Orders Viewer")

df = conn.query("SELECT * FROM orders", ttl=0)

st.dataframe(df)
