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

    # 6. Type widening: INT -> BIGINT keeps old rows readable, new writes wider.
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN amount TYPE BIGINT")
    spark.sql(f"INSERT INTO {table_name} VALUES ('tx3', 3000000000, 'INR')")
    got = {r.id: r.amount for r in spark.table(table_name).select("id", "amount").collect()}
    assert got == {"tx1": 100, "tx2": 200, "tx3": 3000000000}

    # 7. Rename: readers on the new name see all history.
    spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN currency TO txn_currency")
    cols = spark.table(table_name).columns
    assert "txn_currency" in cols and "currency" not in cols
    assert spark.table(table_name).filter("id = 'tx2'").collect()[0].txn_currency == "INR"

    # 8. Drop: old files still read, new writes omit the column.
    spark.sql(f"ALTER TABLE {table_name} DROP COLUMN txn_currency")
    assert "txn_currency" not in spark.table(table_name).columns
    spark.sql(f"INSERT INTO {table_name} VALUES ('tx4', 400)")
    assert spark.table(table_name).count() == 4

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
