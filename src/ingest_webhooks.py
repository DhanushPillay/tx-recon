from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from config import get_spark_session

def run_ingestion():
    spark = get_spark_session("WebhookIngestion")
    
    # Define schema matching our webhook producer
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("amount_paise", IntegerType(), True),
        StructField("gateway_status", StringType(), True),
        StructField("timestamp_utc", StringType(), True),
        StructField("merchant_id", StringType(), True)
    ])
    
    # Read from local Redpanda (Kafka)
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:19092") \
        .option("subscribe", "gateway_webhooks") \
        .option("startingOffsets", "earliest") \
        .load()
        
    # Parse JSON from the Kafka value binary
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Add reconciliation columns
    enriched_df = parsed_df \
        .withColumn("reconciliation_status", col("gateway_status")) \
        .withColumn("bank_ref_id", col("transaction_id")) \
        .withColumn("ingested_at", current_timestamp())
        
    # Write to Iceberg
    # We use a checkpoint location for fault tolerance
    query = enriched_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", "../warehouse/checkpoints/webhooks") \
        .toTable("local.db.webhooks")
        
    print("Started streaming webhooks to Iceberg. Press Ctrl+C to stop.")
    query.awaitTermination()

if __name__ == "__main__":
    # Create the DB if it doesn't exist
    spark = get_spark_session("Init")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    
    # Initialize table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.webhooks (
            transaction_id string,
            amount_paise int,
            gateway_status string,
            timestamp_utc string,
            merchant_id string,
            reconciliation_status string,
            bank_ref_id string,
            ingested_at timestamp
        ) USING iceberg
    """)
    run_ingestion()
