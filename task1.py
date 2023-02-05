#!/usr/bin/env python3
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from datetime import datetime, timedelta
from time import sleep


class HomeworkTask1Job:
    SCRIPT_FILE_DIR = os.path.dirname(os.path.realpath(__file__))
    DEFAULT_INPUT_DIR = os.path.join(SCRIPT_FILE_DIR, "raw_data")
    DEFAULT_OUTPUT_DIR = os.path.join(SCRIPT_FILE_DIR, "results")
    DEFAULT_BATCH_INTERVAL_SECONDS = 60

    TEMP_DIR = "/tmp/homework_task1_job/"
    INPUT_DATE_FORMAT = "%Y%m%d%H%M%S%f"
    RESULT_SCHEMA = StructType(
        [
            StructField("Date", StringType(), False),
            StructField("hour", StringType(), False),
            StructField("impression_count", LongType(), False),
            StructField("click_count", LongType(), False),
        ]
    )

    def __init__(self, input_dir, output_dir, batch_interval_seconds):
        self.input_dir = input_dir if input_dir is not None else self.DEFAULT_INPUT_DIR
        self.output_dir = (
            output_dir if output_dir is not None else self.DEFAULT_OUTPUT_DIR
        )
        self.batch_interval_seconds = (
            batch_interval_seconds
            if batch_interval_seconds is not None
            else self.DEFAULT_BATCH_INTERVAL_SECONDS
        )
        self.spark = SparkSession.builder.appName("Homework task1").getOrCreate()
        self.result_df = self.get_result_df()
        self.batch_data = []
        self.min_dt_for_empty_count = None

    def get_result_df(self):
        """
        Get existing results into dataframe or an empty dataframe
        """
        if os.path.exists(self.output_dir):
            return self.spark.read.csv(
                self.output_dir, schema=self.RESULT_SCHEMA, header=True
            )

        return self.spark.createDataFrame(data=[], schema=self.RESULT_SCHEMA)

    def process_file(self, file_name):
        """
        Process single raw file and store the extracted data into batch_data array
        """
        info_type = file_name.split("_")[0]
        datestring = file_name.split("_")[3]
        dt = datetime.strptime(datestring, self.INPUT_DATE_FORMAT)
        if self.min_dt_for_empty_count is None or self.min_dt_for_empty_count > dt:
            self.min_dt_for_empty_count = dt
        date = dt.strftime("%Y-%m-%d")
        hour = dt.strftime("%H")

        df = self.spark.read.parquet(os.path.join(self.input_dir, file_name))
        count = df.where(df.device_settings.user_agent == "some user agent").count()

        self.batch_data.append(
            (
                date,
                hour,
                count if info_type == "impressions" else 0,
                count if info_type == "clicks" else 0,
            )
        )

    def generate_empty_count_df(self):
        """
        Returns helper dataframe with records for all dates between
        minimum date and now, filled with 0 clicks/impressions, to help fill
        in the results for gaps with no clicks/impressions.
        """
        now = datetime.now()
        data = []
        dt = self.min_dt_for_empty_count or now
        while dt <= now:
            data.append(
                (
                    dt.strftime("%Y-%m-%d"),
                    dt.strftime("%H"),
                    0,
                    0,
                )
            )
            dt += timedelta(hours=1)
        return self.spark.createDataFrame(data=data, schema=self.RESULT_SCHEMA)

    def save_batch_result(self):
        """
        Merges current batch data with existing results and also fills in
        missing date gaps with 0 counts.
        """
        batch_df = self.spark.createDataFrame(
            data=self.batch_data, schema=self.RESULT_SCHEMA
        )
        empty_count_df = self.generate_empty_count_df()

        self.result_df = (
            self.result_df.unionAll(batch_df)
            .unionAll(empty_count_df)
            .groupBy("Date", "hour")
            .sum("impression_count", "click_count")
            .withColumnRenamed("sum(impression_count)", "impression_count")
            .withColumnRenamed("sum(click_count)", "click_count")
            .orderBy("Date", "hour")
        )
        self.result_df.write.csv(
            self.TEMP_DIR,
            mode="overwrite",
            header=True,
        )

        # read back from tmp dir to overwrite result
        self.spark.read.csv(
            self.TEMP_DIR,
            schema=self.RESULT_SCHEMA,
            header=True,
        ).write.csv(
            self.output_dir,
            mode="overwrite",
            header=True,
        )

    def cleanup_after_batch_processing(self, batch_files):
        """
        Remove processed files and reset state for next batch processing
        """
        for file_name in batch_files:
            os.system(f"rm '{os.path.join(self.input_dir, file_name)}'")
        self.batch_data = []

    def process_batch(self):
        """
        Gets a list of new raw data files and processes them
        """
        new_files = os.popen(f"ls '{self.input_dir}'").read().split()
        for file_name in new_files:
            self.process_file(file_name)
        self.save_batch_result()
        self.cleanup_after_batch_processing(new_files)

    def run(self):
        try:
            while True:
                self.process_batch()
                sleep(self.batch_interval_seconds)
        except Exception:
            os.system(f'rm -rf {self.TEMP_DIR}')
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_dir", help="Input directory for raw data")
    parser.add_argument("-o", "--output_dir", help="Output directory for result")
    parser.add_argument(
        "-t",
        "--batch_interval",
        type=int,
        help="How often to check foor new raw data files, in seconds.",
    )
    args = parser.parse_args()

    job = HomeworkTask1Job(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        batch_interval_seconds=args.batch_interval,
    )
    job.run()
