from pyspark.sql import SparkSession
from config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
)
import logging

def get_spark_session(app_name="NYC Taxi PySpark"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    return spark

def load_parquet_from_minio(spark, filename):
    """
    Loads a Parquet file from MinIO (S3) into a Spark DataFrame.
    """
    path = f"s3a://{MINIO_BUCKET}/data/{filename}"
    logging.info(f"Loading data from {path}")
    return spark.read.parquet(path)

def load_reference_data(spark, reference_path):
    """
    Loads the reference CSV data (zone lookup) from MinIO (S3).
    """
    logging.info(f"Loading reference data from {reference_path}")
    return spark.read.csv(reference_path, header=True, inferSchema=True)

# Example usage
if __name__ == "__main__":
    spark = get_spark_session()
    
    # Load the taxi data
    df_taxi = load_parquet_from_minio(spark, "yellow_tripdata_2024-06.parquet")
    df_taxi.printSchema()
    df_taxi.show(5)
    
    # Load reference data
    df_zones = load_reference_data(spark, "s3a://<YOUR_BUCKET>/path/to/zone_lookup.csv")
    df_zones.printSchema()
    df_zones.show(5)