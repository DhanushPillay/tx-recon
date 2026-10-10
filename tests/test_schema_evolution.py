import pytest

from src.common.config import get_spark_session

pytestmark = pytest.mark.integration


def test_iceberg_schema_evolution():
    spark = get_spark_session("SchemaEvolutionTest")
    table_name = "nessie.db.schema_evolution_test"

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.db")

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"""
        CREATE TABLE {table_name} (
            id STRING,
            amount INT
        ) USING iceberg
    """)

    spark.sql(f"INSERT INTO {table_name} VALUES ('tx1', 100)")

    spark.sql(f"ALTER TABLE {table_name} ADD COLUMN currency STRING")

    spark.sql(f"INSERT INTO {table_name} VALUES ('tx2', 200, 'INR')")

    df = spark.table(table_name).orderBy("id").collect()

    assert len(df) == 2
    assert df[0].id == "tx1"
    assert df[0].currency is None
    assert df[1].id == "tx2"
    assert df[1].currency == "INR"

    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN amount TYPE BIGINT")
    spark.sql(f"INSERT INTO {table_name} VALUES ('tx3', 3000000000, 'INR')")
    got = {r.id: r.amount for r in spark.table(table_name).select("id", "amount").collect()}
    assert got == {"tx1": 100, "tx2": 200, "tx3": 3000000000}

    spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN currency TO txn_currency")
    cols = spark.table(table_name).columns
    assert "txn_currency" in cols and "currency" not in cols
    assert spark.table(table_name).filter("id = 'tx2'").collect()[0].txn_currency == "INR"

    spark.sql(f"ALTER TABLE {table_name} DROP COLUMN txn_currency")
    assert "txn_currency" not in spark.table(table_name).columns
    spark.sql(f"INSERT INTO {table_name} VALUES ('tx4', 400)")
    assert spark.table(table_name).count() == 4

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
