import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple, List, Dict
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException

# ---------------- CONFIG ---------------- #
st.set_page_config(page_title="License Patrol 🚓", layout="wide", page_icon="🚨")

# Set up Snowflake connection using st.secrets
session = Session.builder.configs(st.secrets["snowflake"]).create()

# Available semantic models
AVAILABLE_SEMANTIC_MODELS_PATHS = [
    "DP_LCSPTRL.TEST_SNOWPARK.STREAMLIT_CHECK/streamlit_check.yaml"
]

# ---------------- STATE INIT ---------------- #
st.session_state.setdefault("messages", [])
st.session_state.setdefault("query_history", [])
st.session_state.setdefault("mode", "💬 Chat")
st.session_state.setdefault("ad_group_owner_email", "")
st.session_state.setdefault("selected_semantic_model_path", AVAILABLE_SEMANTIC_MODELS_PATHS[0])

# ---------------- EMAIL GATE ---------------- #
if not st.session_state["ad_group_owner_email"]:
    st.title("🔐 License Patrol Access")
    email = st.text_input("Enter your AD Group Owner Email to continue:")
    if email:
        result = session.sql(f"""
            SELECT COUNT(*) AS count
            FROM DP_LCSPTRL.TEST_SNOWPARK.TESTING_STREAMLIT
            WHERE UPPER(app_owner_email_id) = UPPER('{email}')
        """).collect()

        if result and result[0]["COUNT"] > 0:
            st.session_state["ad_group_owner_email"] = email
            st.experimental_rerun()
        else:
            st.error("Unauthorized email. Access denied.")
    st.stop()

# ---------------- SIDEBAR ---------------- #
with st.sidebar:
    st.title("🚨 LICENSE PATROL")
    st.session_state["mode"] = st.radio("Navigate", ["💬 Chat", "📂 View Dataset", "📈 Insights", "🔮 Forecast", "🕓 History"])
    st.selectbox(
        "Semantic Model",
        AVAILABLE_SEMANTIC_MODELS_PATHS,
        key="selected_semantic_model_path",
        format_func=lambda x: x.split("/")[-1]
    )
    st.markdown(f"**Logged in as:** `{st.session_state['ad_group_owner_email']}`")

# ---------------- HANDLERS ---------------- #
def handle_user_inputs():
    user_input = st.chat_input("What is your question?")
    email = st.session_state["ad_group_owner_email"]
    if user_input:
        query = f"{user_input} where app_owner_email_id = '{email}'"
        st.session_state["messages"].append({
            "role": "user",
            "content": [{"type": "text", "text": query}]
        })
        st.session_state["query_history"].append({
            "query": query,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        with st.chat_message("user"):
            st.markdown(query)

        # Simulate Analyst reply
        with st.chat_message("analyst"):
            st.markdown(f"🧠 Responding to query: **{query}**")

def run_chat():
    st.title("💬 Chat")
    for msg in st.session_state["messages"]:
        with st.chat_message(msg["role"]):
            for item in msg["content"]:
                st.markdown(item["text"])
    handle_user_inputs()

def run_view_dataset():
    st.title("📂 View Dataset")
    tables = session.sql("SHOW TABLES").collect()
    table_names = [t["name"] for t in tables]
    selected = st.selectbox("Select table", table_names)
    if selected:
        df = session.table(selected).filter(
            f"UPPER(app_owner_email_id) = UPPER('{st.session_state['ad_group_owner_email']}')"
        ).limit(1000).to_pandas()
        st.dataframe(df)

def run_insights():
    st.title("📈 Data Insights")
    df = session.table("DP_LCSPTRL.TEST_SNOWPARK.TESTING_STREAMLIT").filter(
        f"UPPER(app_owner_email_id) = UPPER('{st.session_state['ad_group_owner_email']}')"
    ).limit(1000).to_pandas()
    st.dataframe(df)
    st.markdown("📊 Simulated Insight: High usage in March and June.")

def run_forecast():
    st.title("🔮 Forecast")
    st.info("ML Forecast coming soon...")

def run_history():
    st.title("🕓 Chat History")
    if not st.session_state["query_history"]:
        st.info("No queries yet.")
    for record in reversed(st.session_state["query_history"]):
        st.markdown(f"**[{record['timestamp']}]** {record['query']}")

# ---------------- ROUTING ---------------- #
mode = st.session_state["mode"]
if mode == "💬 Chat":
    run_chat()
elif mode == "📂 View Dataset":
    run_view_dataset()
elif mode == "📈 Insights":
    run_insights()
elif mode == "🔮 Forecast":
    run_forecast()
elif mode == "🕓 History":
    run_history()
