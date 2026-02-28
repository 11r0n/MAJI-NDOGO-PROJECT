# Paste the entire module code here
"""
Module for processing field data from Maji Ndogo agricultural survey.

This module provides the FieldDataProcessor class which handles data ingestion,
cleaning, transformation, and enrichment of agricultural field data from multiple
sources including SQLite database and weather station CSV files.

The class implements a pipeline for:
- Loading data from SQLite database
- Renaming and correcting column names
- Applying data corrections (absolute elevations, crop name fixes)
- Merging weather station mapping data

Author: ExploreAI
Date: 2024
"""

import pandas as pd
import logging
from data_ingestion import create_db_engine, query_data, read_from_web_CSV


class FieldDataProcessor:
    """
    A class for processing agricultural field data through a configurable pipeline.

    This class orchestrates the entire data processing workflow including loading,
    cleaning, transforming, and enriching field data from multiple sources.

    Attributes:
        db_path (str): Path to the SQLite database
        sql_query (str): SQL query for joining field data tables
        columns_to_rename (dict): Mapping for swapping column names
        values_to_rename (dict): Mapping for correcting crop name spellings
        weather_map_data (str): URL or path to weather station mapping CSV
        logger (logging.Logger): Logger instance for the class
        df (pd.DataFrame): Processed DataFrame
        engine: SQLAlchemy database engine
    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initialize the FieldDataProcessor with configuration parameters.

        Parameters:
            config_params (dict): Dictionary containing all configuration parameters
            logging_level (str): Logging level (DEBUG, INFO, NONE)
        """
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']

        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None

    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of FieldDataProcessor.

        Parameters:
            logging_level (str): Desired logging level
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def ingest_sql_data(self):
        """
        Load data from SQLite database using configured query.

        Creates database engine, executes query, and stores result in self.df.

        Returns:
            pd.DataFrame: The loaded DataFrame
        """
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Successfully loaded data.")
        return self.df

    def rename_columns(self):
        """
        Swap column names based on columns_to_rename configuration.

        Uses a temporary column name to safely swap two column labels.
        """
        # Extract the columns to rename from the configuration
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]

        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})

        self.logger.info(f"Swapped columns: {column1} with {column2}")

    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Apply data corrections to specified columns.

        Converts elevation values to absolute (positive) values and corrects
        crop name spellings using values_to_rename dictionary.

        Parameters:
            column_name (str): Name of the crop type column
            abs_column (str): Name of the elevation column
        """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(
            lambda crop: self.values_to_rename.get(crop, crop)
        )

    def weather_station_mapping(self):
        """
        Merge weather station mapping data with main DataFrame.

        Reads weather station mapping CSV and merges with self.df on Field_ID.
        The resulting 'Weather_station' column contains station IDs.

        Returns:
            pd.DataFrame: The weather station mapping DataFrame
        """
        # Read the weather station mapping data
        weather_mapping_df = read_from_web_CSV(self.weather_map_data)

        # Merge with main DataFrame
        self.df = pd.merge(
            self.df,
            weather_mapping_df[['Field_ID', 'Weather_station']],
            on='Field_ID',
            how='left'
        )

        self.logger.info("Weather station mapping completed successfully.")
        return weather_mapping_df

    def process(self):
        """
        Execute the complete data processing pipeline.

        Calls all processing methods in sequence:
        1. ingest_sql_data() - Load raw data
        2. rename_columns() - Swap column names
        3. apply_corrections() - Clean elevation and crop names
        4. weather_station_mapping() - Add weather station data

        Returns:
            pd.DataFrame: The fully processed DataFrame
        """
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()
        self.logger.info("Data processing complete.")
        return self.df
