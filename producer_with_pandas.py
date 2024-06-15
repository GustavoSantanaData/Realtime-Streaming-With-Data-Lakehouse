import  io
from minio import Minio
import pandas as pd
from minio.error import S3Error
from minio_config import config
from datetime import timedelta

def main():
    """
    Processes Parquet data from a MinIO bucket and uploads it as JSON files.

    This function connects to a MinIO server, retrieves objects from a specified bucket, and processes
    those objects if they match a specific condition. It generates a pre-signed URL to access the data,
    reads the data using pandas, converts each row to JSON format, and uploads the JSON files to another
    MinIO bucket. The purpose of this function is to transform data stored in a Parquet format into a
    more accessible JSON format for downstream applications.

    Steps performed by the function:
    - Connects to the MinIO server with specified credentials.
    - Lists objects in the 'bronze' bucket.
    - Filters objects based on a specific prefix in the object name.
    - Generates a pre-signed URL for accessing the object.
    - Reads the Parquet data from the URL into a pandas DataFrame.
    - Iterates over the DataFrame rows, formats the data, and converts each row to JSON.
    - Uploads each JSON file to the 'nyc-taxis-records' bucket.

    Args:
        None

    Returns:
        None

    Notes:
        - Ensure that the MinIO server is running and accessible at the specified endpoint.
        - The function assumes the presence of the necessary libraries: Minio, pandas, io, etc.
        - Modify bucket names and configurations according to your setup.

    Example:
        >>> main()
        This would connect to the MinIO server, process the 'nyc_taxis_files' objects in the 'bronze'
        bucket, convert the data to JSON, and upload each JSON file to the 'nyc-taxis-records' bucket.
    """
    
    client = Minio(
        endpoint= 'localhost:9000',
        access_key=config['access_key'],
        secret_key=config['secret_key'],
        secure=False
    )

    bucket_name= 'bronze'
    objects = client.list_objects(bucket_name, recursive=True)

    for obj in objects:
        if 'nyc_taxis_files' in obj.object_name:
            url = client.get_presigned_url(
                method='GET',
                bucket_name=bucket_name,
                object_name=obj.object_name,
                expires=timedelta(hours=1)
            )

            data = pd.read_parquet(url)

            for index, row in data.iterrows():
                vendor_id = str(row['VendorID'])
                pickup_datetime = str(row['tpep_pickup_datetime'])
                pickup_datetime_formatted = pickup_datetime.replace(':', '-').replace(' ', '_')
                file_name = f'trip_{vendor_id}_{pickup_datetime_formatted}.json'

                record = row.to_json()
                record_bytes = record.encode('utf-8')
                record_stream = io.BytesIO(record_bytes)
                record_stream_len = len(record_bytes)

                client.put_object(
                    bucket_name='nyc-taxis-records',
                    object_name=f'nyc_taxis_records/{file_name}',
                    data = record_stream,
                    length=record_stream_len,
                    content_type='application/json'
                )

                print(f'Uploaded {file_name} to Minio')
                break
        break




if __name__ == "__main__":
    try:
        main()
    except S3Error as e:
        print('Error ocurred. ', e)