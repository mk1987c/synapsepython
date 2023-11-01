from enum import Enum, unique
from typing import Any, List, Optional, Union
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.window import Window
from typing import Any, Callable, List, NoReturn, Optional, TypeVar, cast
from pyspark.sql.functions import row_number
from pyspark.sql import SparkSession
import datetime

class Dataquality:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    #function to check the duplicate value 
    def check_duplicate_in_columns(
        df: DataFrame,
        filter_condition: Union[str, Column, None] = None,
        column_names: List[str] =[]
    ):
        #global df
        #column_names = ["col1","col2"]

        #using window function to get the duplicate count
        df = df.withColumn("__duplicate_count", F.count("*").over(Window.partitionBy(*column_names)))

        #getting the rownumber for the columns on which we need to validate the duplicate values
        df = df.withColumn("__row_number", row_number().over(Window.partitionBy(*column_names).orderBy(column_names[1])))
        
        expected_condition = (F.col("__duplicate_count") == 1) | (F.col("__row_number") == 1)
        formatted_columns = ", ".join([f"`{col}`" for col in column_names])
        error_message= F.concat(F.lit(f"CHECK_UNIQUE: Column(s) {formatted_columns} has value ("),
                        F.concat_ws(", ", *column_names),
                        F.lit("), which is a duplicate value"))
        return check_dq_conditions(
            df = df,
            expected_condition=expected_condition,
            error_message=error_message,
            filter_condition=filter_condition,
        )

    #function to check the value is not null 

    def check_column_is_not_null(
        df : DataFrame, 
        column_name :str,
        filter_condition: Union[str, Column, None] = None,
    ):
        expected_condition = F.col(column_name).isNotNull()
        error_message= f"CHECK_NOT_NULL: Column `{column_name}` is null"
        return check_dq_conditions(
            df = df,
            expected_condition=expected_condition,
            error_message=error_message,
            filter_condition=filter_condition,
        )
    
    def check_dq_conditions(
        df : DataFrame,
        expected_condition: Union[str, Column],
        error_message: Union[str, Column],
        filter_condition: Union[str, Column, None] = None,
        ):
        global _check_counter

        if isinstance(error_message, str):
            error_message = F.lit(error_message)

        if isinstance(expected_condition, str):
            expected_condition = F.expr(expected_condition)

        if filter_condition is None:
            filter_condition = F.expr("1 = 1")
        elif isinstance(filter_condition, str):
            filter_condition = F.expr(filter_condition)

        _check_counter += 1
        df = df.withColumn(
            f"{_dq}_{_check_counter}",
            F.when(filter_condition & ~expected_condition, error_message)
            .otherwise(F.lit(None).cast("string"))
        )
        return df
       
    def build_df(
        df : DataFrame
    ) -> DataFrame:
            """Obtain the resulting DataFrame with data quality checks applied.

            Returns
            -------
            DataFrame
                The modified PySpark DataFrame with updated validation results.
            """
            temp_dq_cols = [c for c in df.columns if c.startswith("_dq" + "_")]
            print(temp_dq_cols)
            df = (
                df
            .withColumn(
                "_dq",
                F.filter(F.array(*temp_dq_cols), lambda c: c.isNotNull())
            )
            .withColumn(
                "_dq",
                F.when(F.size("_dq") == 0, F.lit(None))
                .otherwise(F.col("_dq"))
            )
            .drop(*temp_dq_cols))  # fmt: skip
            return df
    
    """
    splitting the data frame in where records are correct after validation and in one dataframe
    where records having duplicate , null or incorrect date as per the DQ Validation

    """
    from typing import Tuple
    def split_dataframe_by_dataquality(temp) -> Tuple:

        """
        Splits the input PySpark dataframe into two dataframes: error_df for rows with dq issues and clean_df for rows without dq issues.
        """

        cols_to_drop = ["__duplicate_count", "__row_number"]
        for col_name in cols_to_drop:
            if col_name in temp.columns:
                temp = temp.drop(col_name)
        error_df = temp.filter(col("_dq").isNotNull())
        clean_df = temp.filter(col("_dq").isNull()).drop("_dq")

        return (error_df, clean_df)