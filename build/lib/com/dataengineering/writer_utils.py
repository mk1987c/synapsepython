from pyspark.sql.types import (StructType, ArrayType)
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import StreamingQuery


class writer:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_streaming_data(df: DataFrame, path: str, processing_time: int) -> StreamingQuery:
            """
            Writes streaming data to a Delta Table.

            Args:
                df (DataFrame): Input DataFrame to write.

            Returns:
                StreamingQuery: Streaming query object.

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