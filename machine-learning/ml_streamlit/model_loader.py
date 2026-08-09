import streamlit as st

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.classification import (
    RandomForestClassificationModel
)

# ==========================================================
# PATHS
# ==========================================================

PIPELINE_PATH = "MODEL/feature_pipeline"
MODEL_PATH = "MODEL/random_forest"


# ==========================================================
# LOAD SPARK
# ==========================================================

@st.cache_resource
def load_spark():
    """
    Create Spark Session once.

    Returns
    -------
    SparkSession
    """

    spark = (
        SparkSession.builder
        .appName("FlightDelayPrediction")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ==========================================================
# LOAD FEATURE PIPELINE
# ==========================================================

@st.cache_resource
def load_pipeline():
    """
    Load PipelineModel once.
    """

    pipeline = PipelineModel.load(
        PIPELINE_PATH
    )

    return pipeline


# ==========================================================
# LOAD RANDOM FOREST MODEL
# ==========================================================

@st.cache_resource
def load_model():
    """
    Load RandomForestClassificationModel once.
    """

    model = RandomForestClassificationModel.load(
        MODEL_PATH
    )

    return model


# ==========================================================
# LOAD EVERYTHING
# ==========================================================

spark = load_spark()

pipeline = load_pipeline()

rf_model = load_model()


# ==========================================================
# STARTUP MESSAGE
# ==========================================================

print("=" * 60)
print("Spark Version :", spark.version)
print("Feature Pipeline Loaded")
print("Random Forest Model Loaded")
print("=" * 60)
