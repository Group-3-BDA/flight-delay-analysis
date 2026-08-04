from pyspark.sql import Row

from model_loader import (
    spark,
    pipeline,
    rf_model
)

from feature_builder import build_features


# ==========================================================
# MODEL FEATURES
# ==========================================================

MODEL_FEATURES = [

    "Year",
    "Quarter",
    "Month",
    "DayofMonth",
    "DayOfWeek",

    "DepartureHour",
    "ArrivalHour",

    "PeakHourIndicator",
    "WeekendIndicator",

    "Distance",
    "ScheduledElapsedTimeMinutes",

    "CodeshareFlag",
    "IntraStateRouteFlag",

    "AirlineReliabilityScore",
    "OriginAirportReliabilityScore",
    "DestAirportReliabilityScore",

    "RouteReliabilityScore",

    "AirlineFlightCount",
    "OriginAirportFlightCount",
    "DestAirportFlightCount",

    "RouteFlightCount",

    "RouteAvgDistance",
    "RouteAvgElapsedTime",

    "RouteHistoricalDelayRate",

    "AirlineMonthlyDelayRate",
    "OriginMonthlyDelayRate",
    "DestMonthlyDelayRate",

    "SeasonIndicator_idx",
    "DeparturePeriod_idx",
    "ArrivalPeriod_idx",
    "DistanceCategory_idx"
]


# ==========================================================
# PREDICT
# ==========================================================

def predict_delay(
        airline_code,
        origin_airport,
        destination_airport,
        departure_date,
        departure_time
):
    """
    Predict flight delay.

    Returns
    -------
    dict
    """

    # -----------------------------------------------------
    # Build Features
    # -----------------------------------------------------

    feature_dict = build_features(
        airline_code,
        origin_airport,
        destination_airport,
        departure_date,
        departure_time
    )

    # -----------------------------------------------------
    # Create Spark DataFrame
    # -----------------------------------------------------

    df = spark.createDataFrame(
        [Row(**feature_dict)]
    )

    # -----------------------------------------------------
    # Feature Pipeline
    # -----------------------------------------------------

    transformed = pipeline.transform(df)

    # -----------------------------------------------------
    # Keep only model columns
    # -----------------------------------------------------

    transformed = transformed.select(
        *MODEL_FEATURES,
        "features"
    )

    # -----------------------------------------------------
    # Random Forest Prediction
    # -----------------------------------------------------

    prediction = rf_model.transform(
        transformed
    )

    # -----------------------------------------------------
    # Extract Result
    # -----------------------------------------------------

    row = prediction.select(
        "prediction",
        "probability"
    ).first()

    prediction_value = int(
        row["prediction"]
    )

    probability = row["probability"]

    probability_on_time = float(
        probability[0]
    )

    probability_delay = float(
        probability[1]
    )

    confidence = max(
        probability_on_time,
        probability_delay
    )

    # -----------------------------------------------------
    # Label
    # -----------------------------------------------------

    if prediction_value == 1:

        label = "Delayed"

    else:

        label = "On Time"

    # -----------------------------------------------------
    # Return
    # -----------------------------------------------------

    return {

        "prediction": prediction_value,

        "label": label,

        "delay_probability":
            round(
                probability_delay * 100,
                2
            ),

        "ontime_probability":
            round(
                probability_on_time * 100,
                2
            ),

        "confidence":
            round(
                confidence * 100,
                2
            )

    }

