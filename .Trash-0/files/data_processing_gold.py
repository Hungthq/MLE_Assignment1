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
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType, DecimalType

def process_feature_store(date_str, silver_attributes_daily_directory, silver_financials_daily_directory,
                       silver_clickstream_daily_directory,
                       gold_feature_store_directory, spark):

    # prepare arguments
    #date_str = datetime.strptime(date_str, "%Y-%m-%d")

    # connect to silver lms table
    partition_name = "silver_loan_daily_" + date_str.replace('-','_') + '.parquet'
    filepath = silver_lms_daily_directory + partition_name
    lms = spark.read.parquet(filepath)
    print('loaded from:', filepath, 'row count:', lms.count())

    

    # PII: Name and SSN live in silver attributes but must never reach a feature store
    lms = lms.drop("loan_start_date", "tenure", "installment_num", "", "", "")

			loan_amt	due_amt	paid_amt	overdue_amt	balance	snapshot_date
    print('final row count:', click.count(), '(should equal the click row count after the cutoff filter)')

    # save gold table - IRL connect to database to write
    os.makedirs(gold_feature_store_directory, exist_ok=True)
    filepath = gold_feature_store_directory + "gold_feature_store_" + date_str.replace('-','_') + '.parquet'
    click.write.mode("overwrite").parquet(filepath)
    print('saved to:', filepath)

    return click

def process_label_store(date_str, silver_lms_daily_directory, gold_label_store_directory, spark):

    