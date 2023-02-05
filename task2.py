#!/usr/bin/env python3
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import concat_ws, current_timestamp, to_timestamp
from time import sleep


class HomeworkTask2Job:
    SCRIPT_FILE_DIR = os.path.dirname(os.path.realpath(__file__))
    DEFAULT_INPUT_DIR = os.path.join(SCRIPT_FILE_DIR, "results")
    DEFAULT_LOAD_INTERVAL_SECONDS = 600
    DEFAULT_DB_TABLE = "results"
    DEFAULT_DB_HOST = "localhost"
    DEFAULT_DB_SCHEMA = "task2"
    DEFAULT_DB_USER = "root"
    DEFAULT_DB_PASSWORD = "secret"

    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    INPUT_SCHEMA = StructType(
        [
            StructField("Date", StringType(), False),
            StructField("hour", StringType(), False),
            StructField("impression_count", LongType(), False),
            StructField("click_count", LongType(), False),
        ]
    )

    def __init__(self, input_dir, db_table, load_interval_seconds):
        self.input_dir = input_dir if input_dir is not None else self.DEFAULT_INPUT_DIR
        self.batch_interval_seconds = (
            load_interval_seconds
            if load_interval_seconds is not None
            else self.DEFAULT_LOAD_INTERVAL_SECONDS
        )
        self.spark = (
            SparkSession.builder.appName("Homework task2")
            .config("spark.jars", "mysql-connector-java-8.0.28.jar")
            .getOrCreate()
        )
        self.db_table = db_table if db_table is not None else self.DEFAULT_DB_TABLE
        self.db_host = os.getenv("TASK2_DB_HOST", self.DEFAULT_DB_HOST)
        self.db_schema = os.getenv("TASK2_DB_SCHEMA", self.DEFAULT_DB_SCHEMA)
        self.db_user = os.getenv("TASK2_DB_USER", self.DEFAULT_DB_USER)
        self.db_password = os.getenv("TASK2_DB_PASSWORD", self.DEFAULT_DB_PASSWORD)

    def load_result_to_mysql_db(self):
        """
        Loads task1 results to MySQL DB.
        """
        df = self.spark.read.csv(self.input_dir, schema=self.INPUT_SCHEMA, header=True)

        load_df = (
            df.withColumn(
                "datetime",
                to_timestamp(concat_ws(" ", "Date", "hour"), "yyyy-MM-dd HH"),
            )
            .withColumn(
                "audit_loaded_datetime",
                current_timestamp(),
            )
            .withColumnRenamed("impression_count", "Impression_count")
            .drop("Date", "hour")
        )

        load_df.write.jdbc(
            url=f"jdbc:mysql://{self.db_host}:3306/{self.db_schema}",
            table=self.db_table,
            mode="overwrite",
            properties={
                "driver": "com.mysql.jdbc.Driver",
                "user": self.db_user,
                "password": self.db_password,
            },
        )

    def run(self):
        while True:
            self.load_result_to_mysql_db()
            sleep(self.batch_interval_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_dir", help="Input directory for data")
    parser.add_argument("-d", "--db_table", help="Table name to load data to")
    parser.add_argument(
        "-t",
        "--load_interval",
        type=int,
        help="How often to update destination table, in seconds",
    )
    args = parser.parse_args()

    job = HomeworkTask2Job(
        input_dir=args.input_dir,
        db_table=args.db_table,
        load_interval_seconds=args.load_interval,
    )
    job.run()
