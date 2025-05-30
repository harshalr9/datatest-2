import logging

def write_to_postgres(df, table_name, url, properties):
    """
    Writes a Spark DataFrame to a PostgreSQL table using JDBC.

    Args:
        df (DataFrame): The Spark DataFrame to write.
        table_name (str): The target table name in PostgreSQL.
        url (str): JDBC URL for PostgreSQL.
        properties (dict): JDBC connection properties (user, password, driver).
    """
    try:
        logging.info(f"Writing DataFrame to PostgreSQL table '{table_name}'...")
        df.write.jdbc(url=url, table=table_name, mode="append", properties=properties)
        logging.info("Write successful.")
    except Exception as e:
        logging.error(f"Failed to write DataFrame to PostgreSQL: {e}")
        raise
