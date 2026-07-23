"""
config.py
---------
Configuration file for the Bronze to Silver ETL pipeline.
"""

# ==========================================================
# APPLICATION CONFIGURATION
# ==========================================================

APP_NAME = "Airline_Bronze_To_Silver_ETL"

# ==========================================================
# S3 PATHS
# ==========================================================

# TODO: Replace these with actual S3 paths

BRONZE_PATH = "s3://airline-dataset-2020-2025/Bronze/"

SILVER_PATH = "s3://silver-demo12/Silver/"

# ==========================================================
# FILE FORMATS
# ==========================================================

READ_FORMAT = "csv"

CSV_OPTIONS = {
    "header": "true",
    "inferSchema": "true"
}

# ==========================================================
# REQUIRED COLUMNS
# ==========================================================

# TODO: Replace with the columns required for your Silver layer

REQUIRED_COLUMNS = [
    # "Year",
    # "Month",
    # "DayofMonth",
    # "FlightDate"
]

# ==========================================================
# DATATYPE CONVERSION
# ==========================================================

# TODO: Replace according to your dataset

DATATYPE_MAPPING = {
    # "Year": "int",
    # "Month": "int",
    # "FlightDate": "date"
}

# ==========================================================
# LOGGING
# ==========================================================

LOG_LEVEL = "INFO"

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# ==========================================================
# SPARK CONFIGURATION
# ==========================================================

SHUFFLE_PARTITIONS = "200"

ADAPTIVE_EXECUTION = True
