# Paste the entire module code above here
### START FUNCTION

"""
Module for processing weather station data from Maji Ndogo.

This module provides the WeatherDataProcessor class which handles the extraction,
cleaning, and transformation of weather station data from raw message formats.
It uses regex patterns to extract numeric measurements (temperature, rainfall,
pollution) from unstructured text messages and calculates mean values per
weather station.

Author: ExploreAI
Date: 2024
"""

import re
import logging
from data_ingestion import read_from_web_CSV


class WeatherDataProcessor:
    """
    A class for processing raw weather station message data.

    This class handles the extraction of structured measurement data from
    unstructured text messages received from weather stations. It uses
    configurable regex patterns to identify and extract temperature,
    rainfall, and pollution measurements.

    Attributes:
        weather_station_data (str): URL or path to weather station CSV
        patterns (dict): Dictionary of regex patterns for each measurement type
        logger (logging.Logger): Logger instance for the class
        weather_df (pd.DataFrame): Processed DataFrame with extracted measurements
    """

    def __init__(self, config_params, logging_level="INFO"): # Now we're passing in the config_params dictionary already
        """
        Initialize the WeatherDataProcessor with configuration parameters.

        Parameters:
            config_params (dict): Dictionary containing configuration including
                                 weather_csv_path and regex_patterns
            logging_level (str): Logging level (DEBUG, INFO, NONE)
        """
        self.weather_station_data = config_params['weather_csv_path']
        self.patterns = config_params['regex_patterns']
        self.weather_df = None  # Initialize weather_df as None or as an empty DataFrame
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of WeatherDataProcessor.

        Parameters:
            logging_level (str): Desired logging level
        """
        logger_name = __name__ + ".WeatherDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def weather_station_mapping(self):
        """
        Load weather station data from CSV file.

        Reads the weather station data from the configured URL/path and
        stores it in weather_df attribute.
        """
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.") 
        # Here, you can apply any initial transformations to self.weather_df if necessary.


    def extract_measurement(self, message):
        """
        Extract measurement type and value from a raw message string.

        Applies configured regex patterns to identify and extract numeric
        measurement values from unstructured text messages.

        Parameters:
            message (str): Raw message string from weather station

        Returns:
            tuple: (measurement_type, value) if pattern matches, (None, None) otherwise
        """
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))
        self.logger.debug("No measurement match found.")
        return None, None

    def process_messages(self):
        """
        Process all messages in weather_df to extract measurements.

        Applies extract_measurement to each message in the 'Message' column
        and creates new 'Measurement' and 'Value' columns.

        Returns:
            pd.DataFrame: Updated DataFrame with extracted measurements
        """
        if self.weather_df is not None:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'], self.weather_df['Value'] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is not initialized, skipping message processing.")
        return self.weather_df

    def calculate_means(self):
        """
        Calculate mean values for each measurement type per weather station.

        Groups by weather station ID and measurement type to compute averages.

        Returns:
            pd.DataFrame: Pivot table with weather stations as rows and
                         measurement types as columns
        """
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=['Weather_station_ID', 'Measurement'])['Value'].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            self.logger.warning("weather_df is not initialized, cannot calculate means.")
            return None

    def process(self):
        """
        Execute the complete weather data processing pipeline.

        Steps:
        1. Load weather station data (weather_station_mapping)
        2. Process messages to extract measurements (process_messages)
        3. Log completion
        """
        self.weather_station_mapping()  # Load and assign data to weather_df
        self.process_messages()  # Process messages to extract measurements
        self.logger.info("Data processing completed.")

### END FUNCTION
