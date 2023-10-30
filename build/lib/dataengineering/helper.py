from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

class helper:
    def __init__(self, spark, dataframe):
        self.spark = spark
        self.dataframe = dataframe

    def replace_null_with_zero(self, columns):
        for column in columns:
            self.dataframe = self.dataframe.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column)))

    def get_dataframe(self):
        return self.dataframe

# Initialize a Spark session
spark = SparkSession.builder.appName("DataFrameHelperExample").getOrCreate()

# Assuming you have a Spark DataFrame called 'df' and a list of columns to replace null values with 0.
# Replace 'your_dataframe' and 'your_column_list' with your actual DataFrame and column list.

# Example usage:
# from helper import DataFrameHelper

# df_helper = DataFrameHelper(spark, your_dataframe)
# df_helper.replace_null_with_zero(your_column_list)
# updated_df = df_helper.get_dataframe()
