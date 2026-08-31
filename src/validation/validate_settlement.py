import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors
import os
import glob
import sys

# Define the data contract (schema)
settlement_schema = pa.DataFrameSchema(
    {
        "transaction_id": pa.Column(str, unique=True, nullable=False),
        "settled_amount_paise": pa.Column(int, pa.Check.gt(0), nullable=False),
        "bank_ref_id": pa.Column(str, nullable=False),
    },
    strict=False,  # allow other columns if any
)


def validate_latest_settlement():
    # Find latest csv in data/
    files = glob.glob("data/settlement_*.csv")
    if not files:
        print("No settlement file found.")
        sys.exit(1)

    latest_file = max(files, key=os.path.getctime)
    print(f"Validating {latest_file} with Pandera...")

    df = pd.read_csv(latest_file)

    try:
        settlement_schema.validate(df, lazy=True)
        print("SUCCESS: Data Contract Validated successfully!")
        sys.exit(0)
    except SchemaErrors as err:
        print("ERROR: Data Contract Validation FAILED!")
        for error in err.schema_errors:
            print(f" - {error['error']}")
        sys.exit(1)


if __name__ == "__main__":
    validate_latest_settlement()
