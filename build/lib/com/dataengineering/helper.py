from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

class helper:
    def __init__(self, spark):
        self.spark = spark


    def replace_null_with_zero(self, dataframe, columns):
        for column in columns:
            dataframe = dataframe.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column)))

    def get_dataframe(self):
        return self.dataframe

# Initialize a Spark session
spark = SparkSession.builder.appName("DataFrameHelperExample").getOrCreate()
