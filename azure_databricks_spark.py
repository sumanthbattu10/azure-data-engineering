"""
Azure Databricks Spark Processing
Author: Sumanth Battu
Description: PySpark data processing on Azure Databricks
             with Delta Lake and Medallion Architecture
             for airline operations analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AirlineDataProcessor:
    """
    Azure Databricks PySpark processor for airline data.
    Implements Medallion Architecture (Bronze/Silver/Gold)
    with Delta Lake for ACID transactions and time travel.
    """

    def __init__(self, spark: SparkSession,
                 storage_account: str,
                 container: str):
        self.spark = spark
        self.base_path = (
            f"abfss://{container}@"
            f"{storage_account}.dfs.core.windows.net"
        )
        self._configure_spark()
        logger.info("AirlineDataProcessor initialized")

    def _configure_spark(self):
        """Configure Spark for optimal performance"""
        self.spark.conf.set(
            "spark.sql.adaptive.enabled", "true"
        )
        self.spark.conf.set(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )
        self.spark.conf.set(
            "spark.sql.shuffle.partitions", "200"
        )
        self.spark.conf.set(
            "spark.databricks.delta.optimizeWrite.enabled",
            "true"
        )
        self.spark.conf.set(
            "spark.databricks.delta.autoCompact.enabled",
            "true"
        )
        logger.info("Spark configured for optimal performance")

    def bronze_layer(self,
                     source_path: str,
                     table_name: str) -> None:
        """
        Bronze Layer — Raw data ingestion
        Ingest raw data with minimal transformation
        """
        logger.info(f"Processing Bronze layer: {table_name}")

        raw_df = (self.spark.read
                  .format("parquet")
                  .option("mergeSchema", "true")
                  .load(source_path))

        # Add metadata columns
        bronze_df = raw_df.withColumns({
            "_ingestion_timestamp": F.current_timestamp(),
            "_source_file": F.input_file_name(),
            "_processing_date": F.current_date(),
            "_record_hash": F.sha2(
                F.concat_ws("|", *raw_df.columns), 256
            )
        })

        # Write to Delta Lake Bronze
        bronze_path = f"{self.base_path}/bronze/{table_name}"
        (bronze_df.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .save(bronze_path))

        logger.info(
            f"Bronze layer written: {bronze_df.count()} records"
        )

    def silver_layer(self, table_name: str) -> None:
        """
        Silver Layer — Cleaned and validated data
        Apply data quality rules and transformations
        """
        logger.info(f"Processing Silver layer: {table_name}")

        bronze_path = f"{self.base_path}/bronze/{table_name}"
        bronze_df = self.spark.read.format("delta").load(bronze_path)

        # Data quality validation
        silver_df = (bronze_df
                     .filter(F.col("flight_date").isNotNull())
                     .filter(F.col("flight_number").isNotNull())
                     .filter(F.col("departure_delay") >= -60)
                     .filter(F.col("departure_delay") <= 1440)
                     .dropDuplicates(["flight_id"]))

        # Standardize columns
        silver_df = silver_df.withColumns({
            "flight_date": F.to_date(F.col("flight_date")),
            "departure_delay": F.col("departure_delay").cast(
                IntegerType()
            ),
            "arrival_delay": F.col("arrival_delay").cast(
                IntegerType()
            ),
            "is_delayed": F.when(
                F.col("departure_delay") > 15, True
            ).otherwise(False),
            "delay_category": F.when(
