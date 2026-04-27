# =============================================================================
# common/spark.py
# Centralized Spark session accessor.
# Databricks already provides a running session; getOrCreate() returns it.
# Add any cluster-wide configs here (e.g. timezone, shuffle partitions).
# =============================================================================

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Return the active Spark session, creating one if needed."""
    spark = (
        SparkSession.builder
        .config("spark.sql.session.timeZone", "Asia/Jakarta")
        .config("spark.databricks.unityCatalog.enabled", "true")
        .getOrCreate()
    )
    return spark
