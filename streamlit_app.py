import pandas as pd
import snowflake.connector
import streamlit as st
import mysql.connector

@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

@st.experimental_singleton
def init_starrocks_connection():
    return mysql.connector.connect(**st.secrets["starrocks"])


conn = init_connection()
starrocks_conn = init_starrocks_connection()


@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        return pd.DataFrame.from_records(rows, columns=['timestamp', 'engagement type'])

@st.experimental_memo(ttl=600)
def run_starrocks_query(query):
    with starrocks_conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        return pd.DataFrame.from_records(rows, columns=['c_custkey', 'c_name'])


query = "select c_custkey, c_name from customer limit 10"
st.table(run_starrocks_query(query))


