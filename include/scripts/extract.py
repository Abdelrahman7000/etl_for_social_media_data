import logging
from minio import Minio
import json 
import io
from minio.commonconfig import CopySource

def extract_data():
    """
    This function reads JSON data as a dataframe, then
    loads that data into Minio. 
    
    Args:
        path: the path to the JSON file
            
    """

    minio_client = Minio(
        '192.168.1.7:9000',
        access_key='UaQXEiolL6riDeeUsmQa',
        secret_key='2tsI2HlVX8XyNz9i8GGPmbIRyzQkXyHHqOdn9PNk',
        secure=False
    )

    # Load JSON data from a local file
    with open('/usr/local/airflow/include/social_media_info.json', "r") as f:
        json_data = f.read()

    # Upload the file to MinIO
    minio_client.put_object(
        bucket_name="warehouse",
        object_name="/social_media_info.json",
        data=io.BytesIO(json_data.encode("utf-8")),
        length=len(json_data),
        content_type="application/json"
    )
    
    logging.info('Data loaded into Minio')


if __name__ == "__main__":
    extract_data()
    