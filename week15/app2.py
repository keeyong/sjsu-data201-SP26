import streamlit as st
import pandas as pd

# DB connection (secrets.toml)
conn = st.connection("mysql", type="sql")

st.title("📦 Orders Dashboard")

# ── TAB UI ─────────────────────────────
tab1, tab2 = st.tabs(["📋 Orders Table", "📈 Orders Trend"])

# ──────────────────────────────────────
# TAB 1: Orders Table (existing)
# ──────────────────────────────────────
with tab1:
    st.subheader("All Orders")
    df = conn.query("SELECT * FROM orders", ttl=0)
    st.dataframe(df)

# ──────────────────────────────────────
# TAB 2: Orders Trend (new)
# ──────────────────────────────────────
with tab2:
    st.subheader("Orders Over Time")

    df = conn.query("""
        SELECT DATE(created_at) AS date, COUNT(*) AS orders
        FROM orders
        GROUP BY DATE(created_at)
        ORDER BY date
    """, ttl=0)

    if df.empty:
        st.info("No data available")
    else:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

        st.line_chart(df["orders"])
