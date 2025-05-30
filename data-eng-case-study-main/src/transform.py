import logging
from pyspark.sql.functions import col, to_timestamp

def transform(df_taxi, df_zones):
    """
    Transforms the taxi trip data by joining with the zone lookup table
    and performing basic cleaning.

    Args:
        df_taxi (DataFrame): Raw taxi trip data.
        df_zones (DataFrame): Taxi zone lookup data.

    Returns:
        DataFrame: Transformed DataFrame ready for loading to PostgreSQL.
    """
    logging.info("Starting data transformation...")

    # Example join: enrich pickup and dropoff locations with zone names
    df = df_taxi \
        .join(df_zones.withColumnRenamed("LocationID", "PULocationID")
                        .withColumnRenamed("Zone", "PickupZone"),
              on="PULocationID", how="left") \
        .join(df_zones.withColumnRenamed("LocationID", "DOLocationID")
                        .withColumnRenamed("Zone", "DropoffZone"),
              on="DOLocationID", how="left")

    # Example cleaning: filter out trips with non-positive fare amounts
    df = df.filter(col("fare_amount") > 0)

    logging.info("Transformation complete.")
    return df
