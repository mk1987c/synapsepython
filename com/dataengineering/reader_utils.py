from pyspark.sql.types import (StructType, ArrayType)
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import StreamingQuery

class reader:

    def __init__(self, spark):
        self.spark = spark
        print("started reading the data")

    def read_streaming_data(self, file_format: str, max_files_per_trigger: int, file_extension : str, delimiter : str, schema: StructType, path: str, csv_header: bool) -> DataFrame:        
            """
            Read streaming data from a specified directory using Apache Spark structured streaming.

            Args:
                file_format (str): The format of the input files, e.g., 'csv', 'parquet', 'json', etc.
                max_files_per_trigger (int): The maximum number of files to process per trigger interval.
                file_extension (str): The file extension to filter files in the specified directory, e.g., '.csv', '.json'.
                delimiter (str): The field delimiter for CSV files, e.g., ',' or '\t'.
                schema (pyspark.sql.types.StructType): The schema of the DataFrame.
                path (str): The directory path where streaming data is located.
                csv_header (bool): Whether the input CSV files have a header row use True or false.

            Returns:
                pyspark.sql.DataFrame: A DataFrame containing the streaming data.

            Note:
            - This method is used for reading data from a directory in a streaming fashion using Apache Spark structured streaming.
            - The 'file_format' should be one of the supported formats in Apache Spark, such as 'csv', 'parquet', 'json', etc.
            - 'max_files_per_trigger' controls the number of files to process per streaming trigger interval.
            - 'file_extension' is used to filter files in the specified directory. It should include the dot, e.g., '.csv', '.json'.
            - 'delimiter' is only applicable when 'file_format' is 'csv' and specifies the field delimiter in CSV files.
            - 'schema' defines the structure of the resulting DataFrame and should be specified as a StructType.
            - 'path' is the directory where the streaming data is located.
            - 'csv_header' indicates whether the CSV files have a header row that should be used as column names.

            Example:
            ```
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True)
            ])
            streaming_df = read_streaming_data('csv', 5, '.csv', ',', schema, '/data/streaming', True)
            query = streaming_df.writeStream.outputMode("append").format("console").start()
            query.awaitTermination()
            ```

            See Apache Spark documentation for more information on structured streaming:
            https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
            """
            if file_format.lower() == "csv":
                # Read CSV data
                df = (self.spark.readStream 
                    .option("header", csv_header)
                    .option("maxFilesPerTrigger", max_files_per_trigger)
                    .option("fileNameOnly", "true")
                    .option("pathGlobFilter", file_extension if file_extension else "*")  # Use a default filter if file_format is not defined
                    .option("inferSchema", "false")
                    .option("delimiter", delimiter)
                    .schema(schema)
                    .option("ignoreChanges", "true")
                    .csv(path))
                
            elif file_format.lower() == "xml":
                # Read XML data
                schema_df = self.spark.read.format("xml").load(path).schema
                df = (self.spark.readStream.format("xml")
                    .schema(schema_df)
                    .option("attributePrefix", "")
                    .option("valueTag", "value")
                    .option("rowTag", "row")
                    .load(path))
                
                df = self._flatten_dataframe(df)

            elif file_format.lower() == "json":
                # Read JSON data
                schema_df = self.spark.read.format("json").load(path).schema
                df = (self.spark.readStream.format("json")
                    .schema(schema_df)
                    .option("multiLine", True)
                    .load(path))
                
                df = self._flatten_dataframe(df)

            return df
        

    def flatten_dataframe(df: DataFrame) -> DataFrame:
    # Compute Complex Fields (Lists and Structs) in Schema
        com_fields = dict([(field.name, field.dataType)
                        for field in df.schema.fields
                        if type(field.dataType) == ArrayType or type(field.dataType) == StructType])

        while len(com_fields) != 0:
            col_name = list(com_fields.keys())[0]

            # If StructType then convert all sub-element to columns.
            # i.e. flatten structs
            if type(com_fields[col_name]) == StructType:
                expanded = [F.col(col_name + '.' + cols).alias(col_name.lower() + '_' + cols.lower()) for cols in [keys.name for keys in com_fields[col_name]]]
                df = df.select("*", *expanded).drop(col_name)
            # If ArrayType then add the Array Elements as Rows using the explode function
            # i.e. explode Arrays
            elif type(com_fields[col_name]) == ArrayType:
                df = df.withColumn(col_name, F.explode_outer(col_name))

            # Recompute remaining Complex Fields in Schema
            com_fields = dict([(field.name, field.dataType)
                                for field in df.schema.fields
                                if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
        for cols in df.columns:
            df = df.withColumnRenamed(cols,cols.lower())

        return df
