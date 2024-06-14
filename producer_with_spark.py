from minio import Minio
from minio.error import S3Error
from minio_config import config
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, concat_ws
import json
import io
import os
from pyspark import SparkConf

def main():
    client = Minio(
        endpoint='localhost:9000',
        access_key=config['access_key'],
        secret_key=config['secret_key'],
        secure=False
    )

    bucket_name= 'bronze'
    objects = client.list_objects(bucket_name, recursive=True)

    conf = SparkConf()
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.connection.timeout", "600000")
    conf.set("spark.hadoop.fs.s3a.access.key", config['access_key'])
    conf.set("spark.hadoop.fs.s3a.secret.key", config['secret_key'])
    conf.set("spark.hadoop.fs.s3a.endpoint", "127.0.0.1:9000")
    conf.set("spark.sql.debug.maxToStringFields", "100")
    conf.set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.sdk.disableCertChecking=true")
    conf.set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.sdk.disableCertChecking=true")
    conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-sharing-spark_2.12:3.1.0,mysql:mysql-connector-java:8.0.27")
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    # Criação da sessão Spark
    spark = SparkSession.builder \
        .appName("Kafka Spark Streaming") \
        .master("local[*]") \
        .config(conf=conf) \
        .getOrCreate()
    
    data = spark.read.parquet("s3a://bronze/yellow_tripdata_2023-01.parquet")

    data.show()

    for obj in objects:
        # Checa se 'nyc_taxis_files' está no nome do objeto
        if 'nyc_taxis_files' in obj.object_name:
            # Gera o URL presignado para o objeto
            url = client.presigned_get_object(
                bucket_name,
                obj.object_name
            )

            # Monta o caminho usando o URL presignado
            bucket_path = f"s3a://bronze/{obj.object_name}"

            print(f"URL presignada: {url}")
            print(f"Caminho no formato S3: {bucket_path}")

            try:
                # Read parquet file into Spark DataFrame
                data = spark.read.parquet(bucket_path)

                # Apply transformations using Spark DataFrame API
                transformed_data = data.withColumn(
                    "pickup_datetime_formatted",
                    date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd_HH-mm-ss")
                ).withColumn(
                    "file_name",
                    concat_ws("_", col("VendorID"), col("pickup_datetime_formatted"))
                )

                transformed_data.show()

            except Exception as e:
                print(f"Erro ao ler o arquivo {obj.object_name}: {str(e)}")

            # # Convert DataFrame rows to JSON and upload to MinIO
            # for row in transformed_data.collect():
            #     row_dict = row.asDict()
            #     vendor_id = str(row_dict['VendorID'])
            #     pickup_datetime_formatted = str(row_dict['pickup_datetime_formatted'])
            #     file_name = f'trip_{vendor_id}_{pickup_datetime_formatted}.json'
 
            #     record = json.dumps(row_dict)
            #     record_bytes = record.encode('utf-8')
            #     record_stream = io.BytesIO(record_bytes)
            #     record_stream_len = len(record_bytes)

            #     client.put_object(
            #         bucket_name='nyc-taxis-records',
            #         object_name=f'nyc_taxis_records/{file_name}',
            #         data=record_stream,
            #         length=record_stream_len,
            #         content_type='application/json'
            #     )

            #     print(f'Uploaded {file_name} to Minio')
            #     break


        break

if __name__ == "__main__":
    try:
        main()
    except S3Error as e:
        print('Error occurred.', e)
