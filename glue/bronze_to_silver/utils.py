"""
Utility functions for the Bronze → Silver ETL pipeline.

This module contains reusable transformation functions that operate
on Spark DataFrames.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date


def select_required_columns(df: DataFrame, required_columns: list) -> DataFrame:
    """
    Select only the columns required for the Silver layer.

    Parameters
    ----------
    df : DataFrame
        Input Bronze DataFrame.

    required_columns : list
        List of columns to retain.

    Returns
    -------
    DataFrame
        DataFrame containing only the required columns.
    """

    return df.select(*required_columns)


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardize DataFrame column names by removing unwanted
    spaces and special characters.

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with cleaned column names.
    """

    return df.select(
        [
            col(column).alias(
                column.strip()
                .replace(" ", "_")
                .replace(",", "")
                .replace(";", "")
                .replace("{", "")
                .replace("}", "")
                .replace("(", "")
                .replace(")", "")
                .replace("\n", "")
                .replace("\t", "")
                .replace("=", "_")
            )
            for column in df.columns
        ]
    )


def convert_datatypes(df: DataFrame, datatype_mapping: dict) -> DataFrame:
    """
    Convert DataFrame columns to the required Spark datatypes.

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame.

    datatype_mapping : dict
        Dictionary containing column names and their target datatypes.

    Returns
    -------
    DataFrame
        DataFrame with converted datatypes.
    """

    for column_name, data_type in datatype_mapping.items():

        if column_name in df.columns:

            if data_type == "date":
                df = df.withColumn(column_name, to_date(col(column_name), "yyyy-MM-dd"))

            else:
                df = df.withColumn(column_name, col(column_name).cast(data_type))

    return df


def validate_dataframe(df: DataFrame) -> bool:
    """
    Validate the DataFrame before writing to the Silver layer.

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame.

    Returns
    -------
    bool
        True if the DataFrame is valid.
    """

    return df is not None and len(df.columns) > 0 and df.count() > 0


def write_silver(df: DataFrame, output_path: str) -> None:
    """
    Write the DataFrame to the Silver layer in Parquet format.

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame.

    output_path : str
        Destination path for the Silver layer.

    Returns
    -------
    None
    """

    (
        df.write.mode("overwrite")
        .partitionBy("Year")
        .option("compression", "snappy")
        .parquet(output_path)
    )
