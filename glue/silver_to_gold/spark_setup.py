"""Spark session configuration shared by Glue and EMR."""

from pyspark.sql import SparkSession


def configure_spark(spark: SparkSession, config) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.mergeSchema", "false")
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        str(config.shuffle_partitions),
    )
    spark.conf.set(
        "spark.sql.broadcastTimeout",
        str(config.broadcast_timeout_seconds),
    )
    spark.conf.set(
        "spark.sql.files.maxPartitionBytes",
        str(config.max_partition_bytes),
    )

    version_parts = tuple(int(part) for part in spark.version.split(".")[:2])

    if version_parts >= (3, 0):
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true",
        )
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
