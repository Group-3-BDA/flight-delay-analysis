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

BRONZE_PATHS = [
    "s3://airline-dataset-2020-2025/Bronze/2020/",
    "s3://airline-dataset-2020-2025/Bronze/2021/",
    "s3://airline-dataset-2020-2025/Bronze/2022/",
    "s3://airline-dataset-2020-2025/Bronze/2023/",
    "s3://airline-dataset-2020-2025/Bronze/2024/",
    "s3://airline-dataset-2020-2025/Bronze/2025/",
]

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
    "FlightDate",
    "Year",
    "Quarter",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "Marketing_Airline_Network",
    "Flight_Number_Marketing_Airline",
    "Origin",
    "OriginState",
    "Dest",
    "DestState",
    "CRSDepTime",
    "CRSArrTime",
    "ArrDelay",
    "DepDelay",
    "DepDel15",
    "ArrDel15",
    "CarrierDelay",
    "SecurityDelay",
    "NASDelay",
    "WeatherDelay",
    "LateAircraftDelay",
    "Cancelled",
    "Diverted",
    "Distance",
    "AirTime",
    "TaxiOut",
    "TaxiIn",
    "Operating_Airline",
    "Operated_or_Branded_Code_Share_Partners",
    "OriginStateName",
    "OriginCityName",
    "DestCityName",
    "DestStateName"
]

# ==========================================================
# DATATYPE CONVERSION
# ==========================================================

# TODO: Replace according to your dataset

DATATYPE_MAPPING ={
    "FlightDate": "date",
    "Year": "int",
    "Quarter": "int",
    "Month": "int",
    "DayofMonth": "int",
    "DayOfWeek": "int",
    "Flight_Number_Marketing_Airline": "int",
    "CRSDepTime": "int",
    "CRSArrTime": "int",

    "ArrDelay": "int",
    "DepDelay": "int",
    "DepDel15": "int",
    "ArrDel15": "int",
    "CarrierDelay": "int",
    "SecurityDelay": "int",
    "NASDelay": "int",
    "WeatherDelay": "int",
    "LateAircraftDelay": "int",
    "Cancelled": "int",
    "Diverted": "int",
    "Distance": "int",
    "AirTime": "int",
    "TaxiOut": "int",
    "TaxiIn": "int"
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
