import os

# MinIO (S3) configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "taxi-bucket")

# PostgreSQL configuration
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/taxi_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DRIVER = os.getenv("POSTGRES_DRIVER", "org.postgresql.Driver")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "yellow_tripdata")

# Reference data path in MinIO
REFERENCE_DATA_PATH = os.getenv("REFERENCE_DATA_PATH", "s3a://taxi-bucket/reference-data/taxi_zone_lookup.csv")
