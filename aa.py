from pyspark.sql import SparkSession
from pyspark import SparkConf

# Configurar SparkConf
conf = SparkConf()
conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.connection.timeout", "600000")
conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
conf.set("spark.hadoop.fs.s3a.endpoint", "127.0.0.1:9000")
conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")

# Criação da sessão Spark
spark = SparkSession.builder \
    .appName("Read Parquet from MinIO") \
    .config(conf=conf) \
    .getOrCreate()

# Ler o arquivo Parquet
parquet_file_path = "s3a://bronze/yellow_tripdata_2023-01.parquet"
data = spark.read.parquet(parquet_file_path)

# Mostrar o conteúdo do DataFrame
data.show()
