from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import time

def create_spark_session(jar_path):
    return SparkSession.builder \
        .appName("AppName") \
        .config("spark.jars", jar_path).getOrCreate()


def load_tables(df, table_name,mode= "append",no_of_retries=3):
    """
    Load the data into a PostgreSQL table.
    Args:
        df: The dataframe to load
        table_name: The name of the created table in PostgreSQL.
        mode: The write mode ('append' or 'overwrite').
        no_of_retries: Number of retry attempts on failure.

    """
    # Define the maximum number of retry attempts
    max_retries = no_of_retries
    retry_count = 0

    while retry_count < max_retries:
        try:
            # Write the dataFrame to PostgreSQL
            df.write.mode(mode)\
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option('driver','org.postgresql.Driver')\
                    .option("user", user) \
                    .option("password", password) \
                    .option("dbtable", 'public.'+table_name) \
                    .save()            
            logging.info(f"Successfully loaded data a into {table_name}.")  
            break  # Exit the loop if successful
            
        except Exception as e:
            logging.error(f"Error loading data into {table_name}: {e}")
            retry_count += 1
            time.sleep(3)  
            if retry_count == max_retries:
                logging.critical(f"Failed to load {table_name} into PostgreSQL after {max_retries} attempts.")



def load_data_incrementally(df, table_name,primary_key):
    """
    Load only the new data into PostgreSQL
    Args:
        df: The dataframe to load
        table_name: The name of the created table in PostgreSQL..
        primary_key: The primary key column for identifying existing records

    """
    try:
        existing_records=spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
        logging.info(f"{table_name} exists. Filtering new records...")
        new_records= df.join(existing_records, on=primary_key, how="left_anti")
    except:
        logging.info(f"{table_name} does not exist. Returning all records as new.")
        new_records= df

    mode= 'append' if new_records.count() >0 else 'ignore'
    load_tables(new_records, table_name, mode)



if __name__ == "__main__":
    jdbc_url = "jdbc:postgresql://172.19.0.3:5432/social_media"
    target_driver = "org.postgresql.Driver"
    user='postgres'
    password='postgres'
    targetdb='social_media'
    table_name='user_d'

    connection_properties = {
        "user": "postgres",          
        "password": "postgres",       
        "driver": "org.postgresql.Driver"
    }

    spark = create_spark_session("/usr/local/airflow/include/postgresql-42.7.3.jar")

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    data_specs = [
        ("/usr/local/airflow/include/user_dim.parquet", "user_d", "user_id"),
        ("/usr/local/airflow/include/post_dim.parquet", "post_d", "post_id"),
        ("/usr/local/airflow/include/comment_dim.parquet", "comment_d", "comment_id"),
        ("/usr/local/airflow/include/calendar_dim.parquet", "calendar_d", "date"),
        ("/usr/local/airflow/include/engagement_fact.parquet", "engagement_fact", "post_id"),
    ]
    try:
        
        for path, table, pk in data_specs:
            df = spark.read.format('parquet').load(path)
            load_data_incrementally(df,table,pk)
            logging.info(f"{table} table loaded successfully.")
        logging.info(f"All tables have been loaded successfully.")
    except Exception as e:
        logging.error(f"Error occurred during fetching the data: {e}")
    
    finally:
        spark.stop()
        

