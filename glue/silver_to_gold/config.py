"""Configuration for the Silver-to-Gold Spark pipeline."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    input_path: str
    gold_base_path: str
    output_mode: str = "overwrite"
    train_end_date: str = "2023-12-31"
    validation_year: int = 2024
    test_year: int = 2025
    shuffle_partitions: int = 64
    broadcast_timeout_seconds: int = 900
    max_partition_bytes: int = 128 * 1024 * 1024

    @property
    def fact_flights_path(self) -> str:
        return self.gold_base_path.rstrip("/") + "/FACT_FLIGHTS/"

    @property
    def dim_airline_path(self) -> str:
        return self.gold_base_path.rstrip("/") + "/DIM_AIRLINE/"

    @property
    def dim_airport_path(self) -> str:
        return self.gold_base_path.rstrip("/") + "/DIM_AIRPORT/"

    @property
    def dim_date_path(self) -> str:
        return self.gold_base_path.rstrip("/") + "/DIM_DATE/"

    @property
    def dim_route_path(self) -> str:
        return self.gold_base_path.rstrip("/") + "/DIM_ROUTE/"

    @property
    def ml_dataset_path(self) -> str:
        return self.gold_base_path.rstrip("/") + "/ML_DATASET/"
