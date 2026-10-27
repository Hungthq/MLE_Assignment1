# %%
import os
from datetime import datetime

import pyspark

import utils.data_processing_bronze
import utils.data_processing_silver
import utils.data_processing_gold


# %% [markdown]
############################
# ## set up pyspark session
############################
# %%


# Initialize SparkSession
print("\n" + "#"*50)
print("Initialize SparkSession")
print("#"*50 )
spark = pyspark.sql.SparkSession.builder \
    .appName("dev") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to ERROR to hide warnings
spark.sparkContext.setLogLevel("ERROR")

# %% [markdown]
# ## set up config

# %%
# set up config
print("\n" + "#"*50)
print("Setup config")
print("#"*50)

snapshot_date_str = "2023-01-01"

start_date_str = "2023-01-01"
end_date_str = "2025-01-01"        # last application month in attributes / financials
lms_end_date_str = "2025-11-01"    # last loan snapshot: loans from Jan 2025 reach month 6 in Jul 2025
print("Start date:   ", start_date_str)
print("End date:     ", end_date_str)
print("LMS end date: ", lms_end_date_str)


# %%
# generate list of dates to process
def generate_first_of_month_dates(start_date_str, end_date_str):
    # Convert the date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # List to store the first of month dates
    first_of_month_dates = []

    # Start from the first of the month of the start_date
    current_date = datetime(start_date.year, start_date.month, 1)

    while current_date <= end_date:
        # Append the date in yyyy-mm-dd format
        first_of_month_dates.append(current_date.strftime("%Y-%m-%d"))
        
        # Move to the first of the next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)

    return first_of_month_dates

dates_str_lst = generate_first_of_month_dates(start_date_str, end_date_str)
lms_dates_str_lst = generate_first_of_month_dates(start_date_str, lms_end_date_str)


# %% [markdown]
# ## Build Bronze Table
print("\n" + "#"*50)
print("Build Bronze Table")
print("#"*50 + "\n")
# %%
# create bronze datalake
bronze_lms_directory = "datamart/bronze/lms_loan_daily/"
bronze_clickstream_directory = "datamart/bronze/feature_clickstream/"
bronze_attributes_directory = "datamart/bronze/features_attributes/"
bronze_financials_directory = "datamart/bronze/features_financials/"

if not os.path.exists(bronze_lms_directory):
    os.makedirs(bronze_lms_directory)

if not os.path.exists(bronze_clickstream_directory):
    os.makedirs(bronze_clickstream_directory)

if not os.path.exists(bronze_attributes_directory):
    os.makedirs(bronze_attributes_directory)

if not os.path.exists(bronze_financials_directory):
    os.makedirs(bronze_financials_directory)



# %%
# define the bronze file

lms_file = "data/lms_loan_daily.csv"
clickstream_file = "data/feature_clickstream.csv"
attributes_file = "data/features_attributes.csv"
financials_file = "data/features_financials.csv"

# %%
# run bronze backfill for lms_file
print("\n" + "*"*50)
print("Processing loan data")
print("*"*50 )

for date_str in lms_dates_str_lst:
    utils.data_processing_bronze.process_bronze_table(date_str, lms_file, bronze_lms_directory, spark)

# %%
# run bronze backfill for clickstream feature files
print("\n" + "*"*50)
print("Processing clickstream data")
print("*"*50 )
for date_str in dates_str_lst:
    utils.data_processing_bronze.process_bronze_table(date_str, clickstream_file, bronze_clickstream_directory, spark)

# %%
# run bronze backfill for feature attributes file
print("\n" + "*"*50)
print("Processing attribute data")
print("*"*50 )
for date_str in dates_str_lst:
    utils.data_processing_bronze.process_bronze_table(date_str, attributes_file, bronze_attributes_directory, spark)

# %%
# run bronze backfill for feature financials file
print("\n" + "*"*50)
print("Processing financial data")
print("*"*50 )
for date_str in dates_str_lst:
    utils.data_processing_bronze.process_bronze_table(date_str, financials_file, bronze_financials_directory, spark)

# %% [markdown]
# ## Build Silver Table
print("\n" + "#"*50)
print("Build Silver Table")
print("#"*50 + "\n")
# %%
# create silver datalake for lms
silver_lms_daily_directory = "datamart/silver/lms_loan_daily/"
if not os.path.exists(silver_lms_daily_directory):
    os.makedirs(silver_lms_daily_directory)

# create silver datalake for attributes
silver_attributes_daily_directory = "datamart/silver/features_attributes/"
if not os.path.exists(silver_attributes_daily_directory):
    os.makedirs(silver_attributes_daily_directory)

# create silver datalake for financials
silver_financials_daily_directory = "datamart/silver/features_financials/"
if not os.path.exists(silver_financials_daily_directory):
    os.makedirs(silver_financials_daily_directory)

# create silver datalake for clickstream
silver_clickstream_daily_directory = "datamart/silver/features_clickstream/"
if not os.path.exists(silver_clickstream_daily_directory):
    os.makedirs(silver_clickstream_daily_directory)


# %%
# run silver backfill for lms
print("\n" + "*"*50)
print("Processing loan data")
print("*"*50)
for date_str in lms_dates_str_lst:
    utils.data_processing_silver.process_lms(date_str, bronze_lms_directory, silver_lms_daily_directory, spark)


# %%
# run silver backfill for attributes
print("\n" + "*"*50)
print("Processing attribute data")
print("*"*50)
for date_str in dates_str_lst:
    utils.data_processing_silver.process_attributes(date_str, bronze_attributes_directory, silver_attributes_daily_directory, spark)

# %%
# run silver backfill for financials
print("\n" + "*"*50)
print("Processing financial data")
print("*"*50)
for date_str in dates_str_lst:
    utils.data_processing_silver.process_financials(date_str, bronze_financials_directory, silver_financials_daily_directory, spark)


# %%
# run silver backfill for clickstream
print("\n" + "*"*50)
print("Processing clickstream data")
print("*"*50)
for date_str in dates_str_lst:
    utils.data_processing_silver.process_clickstream(date_str, bronze_clickstream_directory, silver_clickstream_daily_directory, spark)

# EDA on credit labels (bad rate by MOB) has moved to eda.ipynb; run it after this pipeline.

# %% [markdown]
print("\n" + "#"*50)
print("Build Gold Table")
print("#"*50 + "\n")
# ## Build gold table for labels

# %%
# create bronze datalake
gold_label_store_directory = "datamart/gold/label_store/"

if not os.path.exists(gold_label_store_directory):
    os.makedirs(gold_label_store_directory)
    
gold_feature_store_directory = "datamart/gold/feature_store/"
if not os.path.exists(gold_feature_store_directory):
    os.makedirs(gold_feature_store_directory)

# %%
# run gold feature store backfill
print("\n" + "*"*50)
print("Processing feature store")
print("*"*50)
for date_str in dates_str_lst:
    utils.data_processing_gold.process_feature_store(date_str, silver_attributes_daily_directory, silver_financials_daily_directory,
                       silver_clickstream_daily_directory,
                       gold_feature_store_directory, spark)



# %%
# run gold label store backfill
print("\n" + "*"*50)
print("Processing label store")
print("*"*50)
for date_str in lms_dates_str_lst:
    utils.data_processing_gold.process_label_store(date_str, silver_lms_daily_directory, gold_label_store_directory, spark)



