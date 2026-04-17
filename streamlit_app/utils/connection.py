"""Snowflake session helper for Streamlit-in-Snowflake."""

from snowflake.snowpark.context import get_active_session


def get_session():
    """Return the active Snowpark session (available inside Streamlit-in-Snowflake)."""
    return get_active_session()


def run_query(query: str):
    """Execute a SQL query and return the result as a Pandas DataFrame."""
    session = get_session()
    return session.sql(query).to_pandas()
