import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("my_ETL_pipeline").getOrCreate()

def validate_null_values(df, null_columns):
    for column in null_columns:
        if df.filter(col(column).isNull()).count() > 0:
            logging.warning(f"There are null values in column: {column}.")

def validate_non_negative(df, non_negative_columns):
    for column in non_negative_columns:
        if df.filter(col(column) < 0).count() > 0:
            logging.warning(f"There are negative values in column: {column}.")

def check_duplicates(df):
    if df.count() != df.distinct().count():
        logging.warning("Duplicates found in the DataFrame.")
        

def validate_data(df, null_columns = [], non_negative_columns = []):
    check_duplicates(df)
    if null_columns:
        validate_null_values(df, null_columns)
    if non_negative_columns:
        validate_non_negative(df, non_negative_columns)