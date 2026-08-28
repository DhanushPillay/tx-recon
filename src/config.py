import os
from pyspark.sql import SparkSession

def get_spark_session(app_name="TxRecon"):
    # Determine absolute path for local warehouse
    warehouse_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "warehouse"))
    os.makedirs(warehouse_path, exist_ok=True)
    
    # We use local file system for the Iceberg catalog to avoid needing Hadoop/HDFS running locally.
    # In a real production environment, this would point to MinIO/S3 and a Hive/Glue catalog.
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", warehouse_path) \
        .getOrCreate()
        
    return spark
