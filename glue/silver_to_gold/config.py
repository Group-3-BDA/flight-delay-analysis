"""Configuration for the Silver-to-Gold Spark pipeline."""


class PipelineConfig:
    def __init__(
        self,
        input_path,
        gold_base_path,
        output_mode="overwrite",
        train_end_date="2023-12-31",
        validation_year=2024,
        test_year=2025,
        shuffle_partitions=64,
        broadcast_timeout_seconds=900,
        max_partition_bytes=128 * 1024 * 1024
    ):
        self.input_path = input_path
        self.gold_base_path = gold_base_path
        self.output_mode = output_mode
        self.train_end_date = train_end_date
        self.validation_year = validation_year
        self.test_year = test_year
        self.shuffle_partitions = shuffle_partitions
        self.broadcast_timeout_seconds = broadcast_timeout_seconds
        self.max_partition_bytes = max_partition_bytes

    @property
    def fact_flights_path(self):
        return self.gold_base_path.rstrip("/") + "/FACT_FLIGHTS/"

    @property
    def dim_airline_path(self):
        return self.gold_base_path.rstrip("/") + "/DIM_AIRLINE/"

    @property
    def dim_airport_path(self):
        return self.gold_base_path.rstrip("/") + "/DIM_AIRPORT/"

    @property
    def dim_date_path(self):
        return self.gold_base_path.rstrip("/") + "/DIM_DATE/"

    @property
    def dim_route_path(self):
        return self.gold_base_path.rstrip("/") + "/DIM_ROUTE/"

    @property
    def ml_dataset_path(self):
        return self.gold_base_path.rstrip("/") + "/ML_DATASET/"

    @property
    def viz_delay_analytics_path(self):
        return (
            self.gold_base_path.rstrip("/")
            + "/VIZ_DELAY_ANALYTICS/"
        )

    @property
    def viz_reliability_analytics_path(self):
        return (
            self.gold_base_path.rstrip("/")
            + "/VIZ_RELIABILITY_ANALYTICS/"
        )

