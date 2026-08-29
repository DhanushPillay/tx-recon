import pandas as pd
import os
import glob
import sys


def validate_latest_settlement():
    # Find latest csv in data/
    files = glob.glob("data/settlement_*.csv")
    if not files:
        print("No settlement file found.")
        sys.exit(1)

    latest_file = max(files, key=os.path.getctime)
    print(f"Validating {latest_file} with Pandas...")

    df = pd.read_csv(latest_file)
    errors = []

    # 1. Transaction ID must be unique
    if not df["transaction_id"].is_unique:
        errors.append("transaction_id is not unique (contains duplicates).")

    # 2. Settled amount must be strictly positive
    invalid_amounts = df[~df["settled_amount_paise"].between(1, 9999999999)]
    if not invalid_amounts.empty:
        errors.append(
            f"settled_amount_paise contains {len(invalid_amounts)} invalid values."
        )

    # 3. Bank Ref ID must not be null
    null_refs = df[df["bank_ref_id"].isnull()]
    if not null_refs.empty:
        errors.append(f"bank_ref_id contains {len(null_refs)} null values.")

    if not errors:
        print("SUCCESS: Data Contract Validated successfully!")
        sys.exit(0)
    else:
        print("ERROR: Data Contract Validation FAILED!")
        for err in errors:
            print(f" - {err}")
        sys.exit(1)


if __name__ == "__main__":
    validate_latest_settlement()
