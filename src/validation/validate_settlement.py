import glob
import logging
import os
import sys

import pandas as pd
from pandera.errors import SchemaErrors

from .settlement_schema import settlement_schema

logger = logging.getLogger(__name__)


def validate_and_quarantine(df, schema):
    try:
        schema.validate(df, lazy=True)
        return df, pd.DataFrame(columns=df.columns)
    except SchemaErrors as exc:
        failure_idx = exc.failure_cases
        if hasattr(failure_idx, "index") and "index" in failure_idx.columns:
            invalid_mask = df.index.isin(failure_idx["index"].unique())
        else:
            # Fallback: quarantine all rows if failure_cases structure is unexpected
            invalid_mask = pd.Series(True, index=df.index)
        return df[~invalid_mask], df[invalid_mask]


def validate_latest_settlement():
    files = glob.glob("data/settlement_*.csv")
    if not files:
        logger.error("No settlement file found.")
        sys.exit(1)
        return

    latest_file = max(files, key=os.path.getctime)
    logger.info(f"Validating {latest_file} with Pandera...")

    df = pd.read_csv(latest_file)

    _, invalid = validate_and_quarantine(df, settlement_schema)

    quarantine_rate = len(invalid) / len(df) * 100 if len(df) > 0 else 0
    logger.info(
        f"Quarantine rate: {quarantine_rate:.1f}% ({len(invalid)}/{len(df)} rows)"
    )

    if not invalid.empty:
        logger.error("Data Contract Validation FAILED!")
        sys.exit(1)
    else:
        logger.info("SUCCESS: Data Contract Validated successfully!")


if __name__ == "__main__":
    validate_latest_settlement()
