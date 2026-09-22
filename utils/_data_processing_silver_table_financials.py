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


def process_silver_table(snapshot_date_str, bronze_financials_directory, silver_financials_daily_directory, spark):
    # prepare arguments
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    # connect to bronze table
    partition_name = "bronze_daily_" + snapshot_date_str.replace('-','_') + '.csv'
    filepath = bronze_financials_directory + partition_name
    df = spark.read.csv(filepath, header=True, inferSchema=True)
    print('loaded from:', filepath, 'row count:', df.count())

    # clean data: enforce schema / data type
    ## Replace Occupation ______ with "UnSpecified"

    ## Remove "_" and "-" in Annual_Income, cast to int, and get absolute value
    df = df.withColumn(
    "Annual_Income",
    F.regexp_replace(F.col("Annual_Income"), "[_-]", "").cast(DecimalType(10,2))
    )

    ## Rounding Monthly_Inhand_Salary to 2 digits
    df = df.withColumn(
    "Monthly_Inhand_Salary", F.col("Monthly_Inhand_Salary").cast(DecimalType(10,2))
    )

    ## Re-calculate Num_of_Loan
    #df = df.withColumn(
    # 1. Clean out the oxford / conjunction 'and ' so delimiters become purely commas
    #"cleaned_loans",
    #F.regexp_replace(F.trim(F.col("Type_of_Loan")), r",?\s+and\s+", ", ")
    #).withColumn(
    # 2. Split by comma and count elements, handling nulls/empty strings
    #"Num_of_Loan",
    #F.when(
    #    (F.col("Type_of_Loan").isNull()) | (F.trim(F.col("Type_of_Loan")) == ""), 0
    #).otherwise(
    #    F.size(F.split(F.col("cleaned_loans"), r"\s*,\s*"))
    #)
    #).drop("cleaned_loans")

    
    
    
    ## Remove "_" and "-" in Num_of_Delayed_Payment, cast to int, and get absolute value
    df = df.withColumn(
    "Num_of_Delayed_Payment",
    F.regexp_replace(F.col("Num_of_Delayed_Payment"), "[_-]", "").cast(IntegerType())
    )

    # Fill blank Changed_Credit_Limit by "Not Specified"
    df = df.withColumn(
    "Changed_Credit_Limit",
        F.when(
        F.col("Changed_Credit_Limit").isNull()
        | (F.trim(F.col("Changed_Credit_Limit")) == "")
        | (F.col("Changed_Credit_Limit").rlike("^[_\\s]+$")),
        0
    ).otherwise(F.col("Changed_Credit_Limit")).cast(DecimalType(10,2))
    )

    ## Remove "_" and "-" in Outstanding_Debt, cast to int, and get absolute value
    df = df.withColumn(
    "Outstanding_Debt",
    F.regexp_replace(F.col("Outstanding_Debt"), "_", "").cast(DecimalType(10,2))
    )

    ## Remove "_" and "-" in Credit_Utilization_Ratio, cast to int, and get absolute value
    df = df.withColumn(
    "Credit_Utilization_Ratio",
    F.regexp_replace(F.col("Credit_Utilization_Ratio"), "_", "").cast(DecimalType(10,2))
    )

    # Convert Credit History Age to months

    df = df.withColumn(
        "Credit_History_Age_Months",
        (
            F.coalesce(F.regexp_extract(F.col("Credit_History_Age"), r"(\d+)\s*Years?", 1).cast("int"), F.lit(0)) * 12
            + F.coalesce(F.regexp_extract(F.col("Credit_History_Age"), r"(\d+)\s*Months?", 1).cast("int"), F.lit(0))
        )
    )

    # replace min payment from "NM" to "No"
    df = df.withColumn(
    "Payment_of_Min_Amount",
    F.regexp_replace(F.col("Payment_of_Min_Amount"), "NM", "No") 
    )

    # round total EMI 
    df = df.withColumn(
    "Total_EMI_per_month",
    F.col("Total_EMI_per_month").cast(DecimalType(10,2))
    )

    #Amount invest monthly
    df = df.withColumn(
    "Amount_invested_monthly",
    F.regexp_replace(F.col("Amount_invested_monthly"), "_", "").cast(DecimalType(10,2))
    )

    # round monthly balance
    df = df.withColumn(
    "Monthly_Balance",
    F.col("Monthly_Balance").cast(DecimalType(10,2))
    )


    # ---- feature engineering: single-table derived features (no join needed) ----
 
    # Total_Loans: a trustworthy replacement for the still-uncleaned Num_of_Loan (see EDA: 1,850/12,500 mismatch)
    n_cols = ["n_" + t.replace("-", "_").replace(" ", "_") for t in LOAN_TYPES]
    df = df.withColumn("Total_Loans", sum(F.col(c) for c in n_cols))
 
    secured = ["n_Auto_Loan", "n_Mortgage_Loan", "n_Home_Equity_Loan"]
    unsecured = ["n_Personal_Loan", "n_Payday_Loan", "n_Credit_Builder_Loan", "n_Student_Loan"]
    df = df.withColumn("n_secured", sum(F.col(c) for c in secured))
    df = df.withColumn("n_unsecured", sum(F.col(c) for c in unsecured))
    df = df.withColumn("n_distinct_loan_types",
                       sum(F.when(F.col(c) > 0, 1).otherwise(0) for c in n_cols))
 
    # ratio features (null propagates automatically if either side is null/0)
    df = df.withColumn("debt_to_income",
                       F.col("Outstanding_Debt").cast("double") / F.col("Annual_Income").cast("double"))
    df = df.withColumn("emi_to_income",
                       F.col("Total_EMI_per_month").cast("double") / F.col("Monthly_Inhand_Salary").cast("double"))
 
    # Payment_Behaviour ("High_spent_Small_value_payments") -> two separate categorical columns
    df = df.withColumn("Spend_Level", F.regexp_extract(F.col("Payment_Behaviour"), r"^(Low|High)_spent", 1))
    df = df.withColumn("Payment_Value", F.regexp_extract(F.col("Payment_Behaviour"), r"(Small|Medium|Large)_value", 1))
    # regexp_extract returns "" (not null) when the junk value "!@9#%8" doesn't match either pattern
    df = df.withColumn("Spend_Level", F.when(F.col("Spend_Level") == "", "Unknown").otherwise(F.col("Spend_Level")))
    df = df.withColumn("Payment_Value", F.when(F.col("Payment_Value") == "", "Unknown").otherwise(F.col("Payment_Value")))
 
    # Credit_Mix x Payment_of_Min_Amount interaction (EDA: Standard+Yes defaults ~2x Standard+No)
    df = df.withColumn("Credit_Mix_MinAmt", F.concat_ws("_", F.col("Credit_Mix"), F.col("Payment_of_Min_Amount")))

    
    # Dictionary specifying columns and their desired datatypes
    column_type_map = {
        "Customer_ID": StringType(),
        "Annual_Income": DecimalType(10,2),
        "Monthly_Inhand_Salary": DecimalType(10,2),
        "Num_Bank_Accounts": IntegerType(),
        "Num_Credit_Card": IntegerType(),
        "Interest_Rate": IntegerType(),
        "Num_of_Loan": IntegerType(),
        "Type_of_Loan": StringType(),
        "Delay_from_due_date": IntegerType(),
        "Num_of_Delayed_Payment": IntegerType(),
        "Changed_Credit_Limit": DecimalType(10,2),
        "Num_Credit_Inquiries": IntegerType(),
        "Credit_Mix": StringType(),
        "Outstanding_Debt": DecimalType(10,2),
        "Credit_Utilization_Ratio": DecimalType(10,2),
        "Credit_History_Age": StringType(),
        "Payment_of_Min_Amount": StringType(),
        "Total_EMI_per_month": DecimalType(10,2),
        "Amount_invested_monthly": DecimalType(10,2),
        "Payment_Behaviour": StringType(),
        "Monthly_Balance": DecimalType(10,2),
        "snapshot_date": DateType(),
    }

    for column, new_type in column_type_map.items():
        df = df.withColumn(column, col(column).cast(new_type))



    # save silver table - IRL connect to database to write
    partition_name = "silver_daily_" + snapshot_date_str.replace('-','_') + '.parquet'
    filepath = silver_financials_daily_directory + partition_name
    df.write.mode("overwrite").parquet(filepath)
    # df.toPandas().to_parquet(filepath,
    #           compression='gzip')
    print('saved to:', filepath)
    
    return df