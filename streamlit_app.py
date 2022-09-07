import pandas as pd
import snowflake.connector
import streamlit as st


@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])


conn = init_connection()


@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        return pd.DataFrame.from_records(rows, columns=['timestamp', 'engagement type'])


propertyId = st.text_input(label="PropertyId:")

query = "select timestamp, value:propertyEngagementEvent:propertyEngagementType from " \
        "EVENT_INSTRUMENTATION.EVENTS.PROPERTY_LIFECYCLE_MESSAGE " \
        f"where key ilike '{propertyId}' order by timestamp desc;"

st.table(run_query(query))


