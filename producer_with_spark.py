from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from minio import Minio
from minio.error import S3Error
from minio_config import config
import json
import io
from pyspark import SparkConf
from datetime import timedelta

def main():
    """
    Processes data from an S3 bucket and uploads it to a MinIO bucket.

    This function configures a Spark session to read Parquet files from a specified S3 bucket,
    transforms the data, and then uploads it as JSON files to a MinIO bucket. The Spark and MinIO
    client configurations are set to enable secure connection and proper data handling.

    Spark settings include:
    - SSL connection for S3.
    - Access using S3A path style.
    - Connection timeout and endpoint configurations.
    - Specific settings to disable SSL certificate verification.
    - Specification of required JAR packages and Delta SQL extensions.

    After reading and transforming the data:
    - Each row from the Parquet files is processed to extract relevant information.
    - The data is converted to JSON format and uploaded to a specified MinIO bucket.

    Args:
        None

    Returns:
        None

    Notes:
        - Ensure that the necessary libraries are installed and configured correctly.
        - The MinIO endpoint and access keys must be configured properly to ensure a successful connection.

    Example:
        >>> main()
        This would initiate the Spark session, process the data from the specified S3 bucket, and
        upload the processed JSON files to the MinIO bucket.
    """
    
    conf = SparkConf()
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.connection.timeout", "600000")
    conf.set("spark.hadoop.fs.s3a.access.key", config['access_key'])
    conf.set("spark.hadoop.fs.s3a.secret.key", config['secret_key'])
    conf.set("spark.hadoop.fs.s3a.endpoint", "http://172.21.0.3:9000")
    conf.set("spark.sql.debug.maxToStringFields", "100")
    conf.set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.sdk.disableCertChecking=true")
    conf.set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.sdk.disableCertChecking=true")
    conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-sharing-spark_2.12:3.1.0,mysql:mysql-connector-java:8.0.27")
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    # >>> [INFO] Option to increase driver and executor memory
    # conf.set("spark.driver.memory", "4g")
    # conf.set("spark.executor.memory", "4g")
    # conf.set("spark.executor.cores", "2")
    # conf.set("spark.driver.cores", "2")
    # conf.set("spark.executor.instances", "2")


    spark = SparkSession.builder \
        .appName("Kafka Spark Streaming") \
        .master("local[*]") \
        .config(conf=conf) \
        .getOrCreate()

    bucket_name = 'bronze'
    client = Minio(
        endpoint='localhost:9000',
        access_key=config['access_key'],
        secret_key=config['secret_key'],
        secure=False
    )

    objects = client.list_objects(bucket_name, recursive=True)

    for obj in objects:
        if 'nyc_taxis_files' in obj.object_name:
            s3a_path = f"s3a://{bucket_name}/{obj.object_name}"

            data = spark.read.parquet(s3a_path)

            data = data.select(
                col("VendorID").cast("string").alias("VendorID_str"),
                date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd_HH-mm-ss").alias("pickup_datetime_str"),
                "tpep_pickup_datetime",
                "*"
            )

            records = data.collect()

            for row in records:
                vendor_id = row['VendorID_str']
                pickup_datetime = row['pickup_datetime_str']
                row_dict = row.asDict()

                file_name = f'trip_{vendor_id}_{pickup_datetime}.json'
                record_json = json.dumps(row_dict)
                record_bytes = record_json.encode('utf-8')
                record_stream = io.BytesIO(record_bytes)
                record_stream_len = len(record_bytes)

                client.put_object(
                    bucket_name='nyc-taxis-records',
                    object_name=f'nyc_taxis_records/{file_name}',
                    data=record_stream,
                    length=record_stream_len,
                    content_type='application/json'
                )
                print(f'Uploaded {file_name} to Minio')
                break

            break


    spark.stop()

if __name__ == "__main__":
    try:
        main()
    except S3Error as e:
        print('Error occurred.', e)
