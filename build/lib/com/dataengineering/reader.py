from pyspark.sql.types import (StructType, ArrayType)
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import StreamingQuery
from com.Constants import Constants

class reader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

def _read_streaming_data(self, file_format, max_files_per_trigger, file_extension, delimiter, schema, path) -> DataFrame:        
        """
        Read streaming data from various file formats (CSV, JSON, XML) based on the 'file_format' parameter.

        Returns:
            DataFrame: Streaming DataFrame.

        """
        if file_format == Constants.CSV.lower():
            # Read CSV data
            df = (self.spark.readStream 
                .option("header", "false")
                .option("maxFilesPerTrigger", max_files_per_trigger)
                .option("fileNameOnly", "true")
                .option("pathGlobFilter", file_extension if file_extension else "*")  # Use a default filter if file_format is not defined
                .option("inferSchema", "false")
                .option("delimiter", delimiter)
                .schema(schema)
                .option("ignoreChanges", "true")
                .csv(path))
            
        elif file_format == Constants.XML.lower():
            # Read XML data
            schema_df = self.spark.read.format("xml").load(path).schema
            df = (self.spark.readStream.format("xml")
                  .schema(schema_df)
                  .option("attributePrefix", "")
                  .option("valueTag", "value")
                  .option("rowTag", "row")
                  .load(path))
            
            df = self._flatten_dataframe(df)

        elif file_format == Constants.JSON.lower():
            # Read JSON data
            schema_df = self.spark.read.format("json").load(path).schema
            df = (self.spark.readStream.format("json")
                  .schema(schema_df)
                  .option("multiLine", True)
                  .load(path))
            
            df = self._flatten_dataframe(df)

        return df
    

def _flatten_dataframe(df: DataFrame) -> DataFrame:
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
