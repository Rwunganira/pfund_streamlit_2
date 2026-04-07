"""
utils/loaders.py
================
Cached data loaders — one SELECT per mart table, nothing else.
"""

import pandas as pd
import streamlit as st

from utils.db import _get_engine


@st.cache_data(ttl=300, show_spinner=False)
def load_mart_indicator_tracker() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM mart.mart_indicator_tracker", conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_mart_indicator_kpis() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM mart.mart_indicator_kpis", conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_mart_entity_performance() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM mart.mart_entity_performance", conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_mart_strategic_summary() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM mart.mart_strategic_summary", conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_mart_budget_performance() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM mart.mart_budget_performance", conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_mart_activity_status() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM mart.mart_activity_status", conn)
