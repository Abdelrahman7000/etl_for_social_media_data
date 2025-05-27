from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from data_quality_checks import *
from minio import Minio
import json 
import io
import ast


def process_extracted_data(spark):
    """
    Process the extracted data from a Parquet file into a dataframe.

    Args:
        spark: The Spark session instance.

    Returns:
        Transformed DataFrame.
    """
    
    minio_client= Minio(
            '192.168.1.7:9000',
            access_key='UaQXEiolL6riDeeUsmQa',
            secret_key='2tsI2HlVX8XyNz9i8GGPmbIRyzQkXyHHqOdn9PNk',
            secure=False
        )
    try:
        #df = spark.read.parquet('/home/jovyan/work/extracted_data.parquet')
        response=minio_client.get_object('warehouse','/social_media_info.json')
        data = response.read() 
        records= json.loads(data.decode('utf-8'))
        df = spark.createDataFrame(records)

        logging.info("Data extracted successfully.")
    except Exception as e:
        logging.error(f"Error in data extraction: {e}")
    finally:
        response.close()
        response.release_conn()

    logging.info("Data transformation processes started")
    detailed_df=df.withColumn('post',explode_outer('posts')).select('age','date_created','email','gender','name','user_id','username','post')
    
    new_df=detailed_df.select(col('age'),
                   to_timestamp(col('date_created')).alias('date_created'),
                   col('email'),
                   col('gender'),
                   col('name'),
                   col('user_id'),
                   col('username'),
                   col('post.post_id'),
                   col('post.location').alias('post_location'),
                   col('post.post_text'),
                   col('post.reactions').alias('post_reaction'),
                   col('post.shares').alias('no_post_shares'),
                   col('post.tags').alias('post_tags'),
                   to_timestamp(col('post.timestamp')).alias('post_timestamp'),
                   col('post.comments').alias('post_comments')
               )
    
    return new_df



def create_user_dim(df):
    """
    Create the user dimension dataframe.

    Args:
        df: The DataFrame containing user data.

    Returns:
        User dimension dataFrame with users' related data.
    """
    user_dim=df.select(col('user_id'),
                       col('username'),
                       col('name'),
                       col('gender'),
                       col('email'),
                       col('age').cast('integer')
                      )

    # Keep distinct user records
    user_dimension=user_dim.distinct()
    validate_data(user_dimension,null_columns=['user_id','username'],non_negative_columns=['age'])
    
    return user_dimension



def create_post_dim(df):
    """
    Create the post dimension DataFrame.

    Args:
    df: The DataFrame containing post data.

    Returns:
        Post dimension dataFrame with posts' related data.
    """
    post_dim=df.select(col('post_id'),
                      col('post_text'),
                      col('post_location'),
                      col('post_tags')
                      )

    post_dimension=post_dim.na.drop('all')
    validate_data(post_dimension, null_columns=["post_id", "post_text"])

    return post_dimension


def create_comment_dim(df):
    """
    Create the comment dimension DataFrame.

    Args:
    df: The DataFrame containing comment data.

    Returns:
        Comment dimension dataFrame with exploded comments' data.
    """
    new_df=df.select(col('post_id'),col('post_comments'))

    def clean_comments(text):
        if text == 'NULL':
            return ['NULL']
        if text is None:
            return None
        cleaned = text.replace('[','').replace(']','').replace('{','')\
                    .replace('comment=','').replace('user_id=','')\
                    .replace('timestamp=','').replace('},','$')
        return cleaned.split('$')

    # Register UDF
    clean_comments_udf = udf(clean_comments, ArrayType(StringType()))

    # Apply UDF and explode
    new_df = new_df.withColumn("fixed_comments", clean_comments_udf(col("post_comments")))
    new_df = new_df.withColumn("fixed_comment", explode(col("fixed_comments")))

    # Split exploded string into components
    split_cols = split(col("fixed_comment"), ",")
    new_df = new_df.withColumn("comment", split_cols.getItem(0)) \
        .withColumn("user_id", split_cols.getItem(1)) \
        .withColumn("timestamp", split_cols.getItem(2))

    # Final selection
    result_df = new_df.select("post_id", "comment", "user_id", "timestamp")
    result_df=result_df.select(col('post_id'),
                                   col('comment').alias('comment_text'),
                                   col('user_id'),
                                   to_timestamp(col('timestamp')).alias('comment_timestamp')
                                )
    
    comment_dim_with_id = result_df.withColumn("comment_id",sha2(
        concat_ws("||",
                  col("user_id").cast("string"),
                  col("post_id").cast("string"),
                  coalesce(col("comment_timestamp").cast("string"), lit("null"))
        ), 256)
)   
    
    
    validate_data(comment_dim_with_id,null_columns=['post_id','user_id','comment_timestamp'])
    return comment_dim_with_id


    



def create_calendar_dim(fact_table,comment_dim):
    """
    Create the calendar dimension DataFrame.

    Args:
    fact_table: The DataFrame containing the fact table data.
    comment_dim: The DataFrame containing the comment dimension data.

    Returns:
        Calendar dimension DataFrame with timestamp details.
    """
    
    fact_post_timestamps=fact_table.select('post_timestamp').alias('datetime').distinct()
    fact_user_timestamps=fact_table.select('user_created_date').alias('datetime').distinct()
    comment_timestamps=comment_dim.select('comment_timestamp').alias('datetime').distinct()

    # Combine all distinct timestamps into a single DataFrame
    combined_timestamps = (
        fact_post_timestamps.union(fact_user_timestamps).union(comment_timestamps).distinct()).withColumnRenamed('post_timestamp','datetime')


    calendar_dimension = combined_timestamps.withColumn("date", to_date(col("datetime"))) \
                                   .withColumn("year", year(col("date"))) \
                                   .withColumn("month", month(col("date"))) \
                                   .withColumn("day", expr("day(datetime)")) \
                                   .withColumn("weekday", dayofweek(col("date"))) \
                                   .withColumn("month_name", date_format(col("date"), 'MMMM')) \
                                   .withColumn("quarter", expr("quarter(date)")) \
                                   .withColumn("hour", hour(col("datetime"))) \
                                   .withColumn("minute", minute(col("datetime")))
    
    validate_data(calendar_dimension)
    return calendar_dimension
    


def create_fact_table(df):
    """
    Create the engagement fact table DataFrame.

    Args:
    df: The DataFrame containing engagement data.

    Returns:
        Engagement fact table DataFrame that contains various matrics.
    """
    def clean_reactions(text):
        if text == 'NULL':
            return ['NULL']
        if text is None:
            return None

        text = text.replace('=', ':')
        items = text.strip('{}').split(',')
        pairs = []

        for item in items:
            if ':' not in item:
                continue  # Skip malformed entry
            key, value = item.split(':', 1)  # Use maxsplit=1 to prevent issues if value has a colon
            pairs.append(f'"{key.strip()}":{value.strip()}')

        fixed_str = '{' + ', '.join(pairs) + '}'

        try:
            data_dict = ast.literal_eval(fixed_str)
            return data_dict
        except Exception:
            return None
   
    def clean_comments(text):
        if text == 'NULL':
            return ['NULL']
        if text is None:
            return None
        cleaned = text.replace('[','').replace(']','').replace('{','')\
                    .replace('comment=','').replace('user_id=','')\
                    .replace('timestamp=','').replace('},','$')
        return cleaned.split('$')
    
    # clean the reactions column
    edited_df=df.select('post_id','user_id','post_timestamp','date_created','post_reaction','no_post_shares','post_comments','post_tags').distinct()
    clean_reactions_udf = udf(clean_reactions, MapType(StringType(), IntegerType()))
    edited_df = edited_df.withColumn("post_reaction", clean_reactions_udf(col("post_reaction")))
    new_df=edited_df.select('post_id','user_id','post_timestamp','date_created','no_post_shares','post_comments','post_tags','post_reaction',explode(edited_df.post_reaction))
    # Transpose the reactions into columns and sum its values
    new_df=new_df.groupBy('post_id','user_id','post_timestamp','date_created','no_post_shares','post_comments','post_tags').pivot('key').agg(
    sum('value').alias("reactions_count"))
    
    # clean the post_comments column
    clean_comments_udf = udf(clean_comments, ArrayType(StringType()))
    new_df_2 = new_df.withColumn("post_comments", clean_comments_udf(col("post_comments")))
    new_df_2 = new_df_2.withColumn("post_comments", explode(col("post_comments")))
    new_df_2=new_df_2.groupBy('post_id','user_id','post_timestamp','date_created','no_post_shares','post_tags','angry','haha','like','love','sad','wow').agg(
    count('post_comments').alias("comments_count"))


    # Get the number of tags for each post
    cleaned_tags = regexp_replace(col("post_tags"), r"[\[\]]", "")  # remove brackets
    split_tags = split(cleaned_tags, r"\s*,\s*")  # split by comma with optional whitespace
   
    # Handle empty string case properly
    final_df = new_df_2.withColumn("tags_count", when(trim(cleaned_tags) == "", 0).otherwise(size(split_tags)))
    final_df=final_df.select(
                    col('post_id'),
                    col('user_id'),
                    col('post_timestamp'),
                    col('date_created').alias('user_created_date'),
                    col('no_post_shares'),
                    col('angry').alias('angry_reaction_count'),
                    col('haha').alias('haha_reaction_count'),
                    col('like').alias('like_reaction_count'),
                    col('love').alias('love_reaction_count'),
                    col('sad').alias('sad_reaction_count'),
                    col('wow').alias('wow_reaction_count'),
                    col('comments_count'),
                    col('tags_count')
    )
    # drop the rows with no post_id or user_id
    columns_to_check = ["user_id","post_id"]
    final_df = final_df.dropna(subset=columns_to_check)
    
    # Fill null values with zero in the specified columns
    reactions= ['angry','haha','like','love','sad','wow']
    columns_to_check = [f'{reaction}_reaction_count' for reaction in reactions] + ['no_post_shares', 'comments_count','tags_count']
    
    fact_table = final_df.fillna(0, subset=columns_to_check)

    # do some quality checks
    validate_data(fact_table,null_columns=['user_id','post_id'],non_negative_columns=columns_to_check)

    return fact_table


def get_spark_session():
    """
    Get the active Spark session or create a new one if none exists.

    Returns:
    SparkSession: The Spark session instance.
    """
    if SparkSession._instantiatedSession is not None:
        return SparkSession._instantiatedSession
    else:
        return SparkSession.builder.appName("my_ETL_pipeline")\
                                   .config("spark.driver.memory", "4g")\
                                   .config("spark.executor.memory", "4g")\
                                   .getOrCreate()

    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
 
    spark = get_spark_session()
    try:
        df= process_extracted_data(spark)
        
        functions=[(create_user_dim,'/usr/local/airflow/include/user_dim.parquet'),
                   (create_post_dim,'/usr/local/airflow/include/post_dim.parquet'),
                   (create_comment_dim,'/usr/local/airflow/include/comment_dim.parquet'),
                   (create_fact_table,'/usr/local/airflow/include/engagement_fact.parquet')]
        
        stk=[]
        for fn,path in functions:
            res=fn(df)
            stk.append(res)
            res.repartition(10).write.parquet(path,mode='overwrite')
        
     
        calendar_dim=create_calendar_dim(stk[-1],stk[-2])
        calendar_dim.write.parquet('/usr/local/airflow/include/calendar_dim.parquet',mode='overwrite')


        
        logging.info('Files have been written successfully')

        print('#'*1000)

    except Exception as e:
        logging.error(f"Error has been occurred: {e}")

    finally:
        spark.stop()

  
    
