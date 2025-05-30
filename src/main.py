import logging
from pyspark.sql import SparkSession
from config import *
from load_data import load_parquet_from_minio, load_reference_data
from transform import transform
from write_to_posgres import write_to_postgres

def main():
    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder \
        .appName("NYC Taxi Data Pipeline") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.27") \
        .getOrCreate()

    # Load data
    df_taxi = load_parquet_from_minio(spark, "yellow_tripdata_2024-06.parquet")
    df_zones = load_reference_data(spark)

    # Transform data
    df_transformed = transform(df_taxi, df_zones)

    # Prepare JDBC properties
    properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": POSTGRES_DRIVER
    }

    # Write to PostgreSQL
    write_to_postgres(df_transformed, POSTGRES_TABLE, POSTGRES_URL, properties)

    logging.info("Pipeline execution completed.")

if __name__ == "__main__":
    main()

