"""
Module for ingesting and merging agricultural survey data from Maji Ndogo.

This module provides functionality to connect to the SQLite database containing
farm survey data, execute SQL queries to merge multiple tables, and load weather
data from remote CSV sources. It sets up logging to track the data ingestion process.

The main dataset includes geographic, weather, soil, and farm management features
for various fields in Maji Ndogo, all linked by Field_ID.

Author: ExploreAI
Date: 2024
"""

from sqlalchemy import create_engine, text
import logging
import pandas as pd

# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')

# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')



def create_db_engine(db_path):
    """
    Creates a database engine connection to the SQLite database.

    Parameters:
    -----------
    db_path : str
        The file path or connection string to the SQLite database.

    Returns:
    --------
    sqlalchemy.engine.base.Engine
        A SQLAlchemy engine object connected to the specified database.

    Raises:
    -------
    ImportError
        If SQLAlchemy is not installed in the environment.
    Exception
        If the database connection fails for any other reason.
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        logger.info("Database engine created successfully.")
        return engine
    except ImportError:
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise
    except Exception as e:
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e

def query_data(engine, sql_query):
    """
    Executes a SQL query against the database and returns the results as a DataFrame.

    Parameters:
    -----------
    engine : sqlalchemy.engine.base.Engine
        A SQLAlchemy engine object connected to the database.
    sql_query : str
        A valid SQL query string to execute on the database.

    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing the results of the SQL query.

    Raises:
    -------
    ValueError
        If the query returns an empty DataFrame.
    Exception
        If the query execution fails for any other reason.
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e

def read_from_web_CSV(URL):
    """
    Reads a CSV file from a web URL into a pandas DataFrame.

    Parameters:
    -----------
    URL : str
        The complete web URL pointing to a CSV file.

    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing the data from the CSV file.

    Raises:
    -------
    pd.errors.EmptyDataError
        If the URL does not point to a valid CSV file or the file is empty.
    Exception
        If the file cannot be read for any other reason.
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
