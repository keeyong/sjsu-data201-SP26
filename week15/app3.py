import streamlit as st
import pandas as pd

# ── DATABASE CONNECTION ─────────────────────────────
# Uses credentials from .streamlit/secrets.toml
conn = st.connection("mysql", type="sql")

# ── PAGE TITLE ──────────────────────────────────────
st.title("📦 Orders Dashboard")

# ── TABS UI ─────────────────────────────────────────
# Tab 1: Table view
# Tab 2: Orders over time
# Tab 3: Orders grouped by status
tab1, tab2, tab3 = st.tabs([
    "📋 Orders Table",
    "📈 Orders Trend",
    "📊 Status Breakdown"
])


# ════════════════════════════════════════════════════
# TAB 1: SHOW ALL ORDERS (READ-ONLY TABLE)
# ════════════════════════════════════════════════════
with tab1:
    st.subheader("All Orders")

    # Fetch all orders from DB
    df = conn.query("SELECT * FROM orders", ttl=0)

    # Display as interactive table
    st.dataframe(df)


# ════════════════════════════════════════════════════
# TAB 2: ORDERS OVER TIME (LINE CHART)
# ════════════════════════════════════════════════════
with tab2:
    st.subheader("Orders Over Time")

    # Aggregate orders by date
    df = conn.query("""
        SELECT DATE(created_at) AS date, COUNT(*) AS orders
        FROM orders
        GROUP BY DATE(created_at)
        ORDER BY date
    """, ttl=0)

    if df.empty:
        st.info("No data available")
    else:
        # Convert to datetime and set index for chart
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

        # Display line chart
        st.line_chart(df["orders"])


# ════════════════════════════════════════════════════
# TAB 3: ORDERS BY STATUS (BAR CHART)
# ════════════════════════════════════════════════════
with tab3:
    st.subheader("Orders by Status")

    # Count orders grouped by status
    df = conn.query("""
        SELECT status, COUNT(*) AS count
        FROM orders
        GROUP BY status
    """, ttl=0)

    if df.empty:
        st.info("No data available")
    else:
        # Use status as index for visualization
        df = df.set_index("status")

        # Display bar chart
        st.bar_chart(df["count"])
