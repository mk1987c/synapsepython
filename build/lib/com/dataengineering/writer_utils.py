from pyspark.sql.types import (StructType, ArrayType)
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import StreamingQuery


class writer:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_streaming_data(self, df: DataFrame, path: str, processing_time: str) -> StreamingQuery:
            """
            Writes streaming data from a DataFrame to a Delta Table using structured streaming.

            Args:
                df (DataFrame): The input DataFrame containing the streaming data to be written to the Delta Table.
                path (str): The path to the Delta Table where the streaming data will be stored.
                processing_time (str): The processing time interval in seconds for triggering the streaming query.

            Returns:
                StreamingQuery: A StreamingQuery object representing the started streaming query.

            Notes:
            - This method is used to write streaming data to a Delta Table using Apache Spark structured streaming.
            - 'df' should be a DataFrame that represents the streaming data you want to write.
            - 'path' should be the path to the Delta Table where the data will be stored. The path can be an HDFS or local file system path.
            - 'processing_time' specifies the trigger interval for processing streaming data. It determines how often the streaming query will write data to the Delta Table ex :- '2 seconds'.
            """
            query = (
                df
                .writeStream
                .format("delta")
                .outputMode("append")
                .partitionBy('processing_date')
                .option("path", path)
                .option("checkpointLocation", f"{path}/cp_loc/")
                .trigger(processingTime= processing_time)\
                .start()
            )
            return query    