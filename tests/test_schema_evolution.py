import pytest

from src.common.config import get_spark_session

pytestmark = pytest.mark.integration


def test_iceberg_schema_evolution():
    """
    Test that Iceberg table can handle schema evolution (adding a new column).
    """
    spark = get_spark_session("SchemaEvolutionTest")
    table_name = "nessie.db.schema_evolution_test"

    # Nessie requires the namespace to exist before CREATE TABLE.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.db")

    # 1. Create table with initial schema
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"""
        CREATE TABLE {table_name} (
            id STRING,
            amount INT
        ) USING iceberg
    """)

    # 2. Insert data
    spark.sql(f"INSERT INTO {table_name} VALUES ('tx1', 100)")

    # 3. Evolve schema (Add new column)
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMN currency STRING")

    # 4. Insert data with new schema
    spark.sql(f"INSERT INTO {table_name} VALUES ('tx2', 200, 'INR')")

    # 5. Verify reads
    df = spark.table(table_name).orderBy("id").collect()

    assert len(df) == 2
    assert df[0].id == "tx1"
    assert df[0].currency is None
    assert df[1].id == "tx2"
    assert df[1].currency == "INR"
