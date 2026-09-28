import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import pyspark.sql.functions as F
import argparse

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType


def process_silver_table(snapshot_date_str, bronze_attributes_directory, silver_attributes_daily_directory, spark):
    # prepare arguments
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    # connect to bronze table
    partition_name = "bronze_daily_" + snapshot_date_str.replace('-','_') + '.csv'
    filepath = bronze_attributes_directory + partition_name
    df = spark.read.csv(filepath, header=True, inferSchema=True)
    print('loaded from:', filepath, 'row count:', df.count())

    # clean data: enforce schema / data type
    ## Replace Occupation ______ with "UnSpecified"
    
    df = df.withColumn(
    "Occupation",
    F.when(F.col("Occupation") == "_______", "Unspecified").otherwise(F.col("Occupation"))
    )

    ## Remove "_" and "-" in Age, cast to int, and get absolute value
    df = df.withColumn(
    "Age",
    F.abs(F.regexp_replace(F.col("Age"), "[_-]", "").cast(IntegerType()))
    )
    # values that survive stripping but are still not a real age (e.g. "-500" -> 500, "3843_" -> 3843)
    # get set to null rather than kept as a wrong number
    df = df.withColumn(
        "Age",
        F.when(F.col("Age").between(14, 100), F.col("Age"))
    )
    
    # Dictionary specifying columns and their desired datatypes
    column_type_map = {
        "Customer_ID": StringType(),
        "Name": StringType(),
        "Age": IntegerType(),
        "SSN": StringType(),
        "Occupation": StringType(),
        "snapshot_date": DateType(),
    }

    for column, new_type in column_type_map.items():
        df = df.withColumn(column, col(column).cast(new_type))



    # save silver table - IRL connect to database to write
    partition_name = "silver_daily_" + snapshot_date_str.replace('-','_') + '.parquet'
    filepath = silver_attributes_daily_directory + partition_name
    df.write.mode("overwrite").parquet(filepath)
    # df.toPandas().to_parquet(filepath,
    #           compression='gzip')
    print('saved to:', filepath)
    
    return df