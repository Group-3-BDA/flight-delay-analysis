"""Gold-layer output writers."""

from pyspark.sql import DataFrame


def write_gold_outputs(
    fact_flights_df: DataFrame,
    dim_airline_df: DataFrame,
    dim_airport_df: DataFrame,
    dim_date_df: DataFrame,
    dim_route_df: DataFrame,
    ml_dataset_df: DataFrame,
    viz_delay_analytics_df: DataFrame,
    viz_reliability_analytics_df: DataFrame,
    config,
) -> None:
    # Write dimensions first. The visualization tables reuse these dimensions.
    dim_date_df.write.mode(config.output_mode).parquet(config.dim_date_path)
    dim_airline_df.write.mode(config.output_mode).parquet(config.dim_airline_path)
    dim_airport_df.write.mode(config.output_mode).parquet(config.dim_airport_path)
    dim_route_df.write.mode(config.output_mode).parquet(config.dim_route_path)

    (
        fact_flights_df.write.mode(config.output_mode)
        .partitionBy("Year", "Month")
        .parquet(config.fact_flights_path)
    )

    (
        ml_dataset_df.write.mode(config.output_mode)
        .partitionBy("Year", "Month")
        .parquet(config.ml_dataset_path)
    )

    (
        viz_delay_analytics_df.write.mode(config.output_mode)
        .partitionBy("Year", "Month")
        .parquet(config.viz_delay_analytics_path)
    )

    (
        viz_reliability_analytics_df.write.mode(config.output_mode)
        .partitionBy("Year", "Month")
        .parquet(config.viz_reliability_analytics_path)
    )
