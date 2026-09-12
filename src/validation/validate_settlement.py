import glob
import logging
import os

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

from src.validation.settlement_schema import settlement_schema

logger = logging.getLogger(__name__)


class SettlementValidationError(Exception):
    pass


def validate_and_quarantine(
    df: pd.DataFrame, schema: pa.DataFrameSchema
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split df into (valid, invalid) rows. Fails closed with an explicit error
    if pandera's failure_cases shape is unexpected (never silently quarantine all)."""
    try:
        schema.validate(df, lazy=True)
        return df, pd.DataFrame(columns=df.columns)
    except SchemaErrors as exc:
        failure_idx = exc.failure_cases
        if hasattr(failure_idx, "index") and "index" in failure_idx.columns:
            invalid_mask = df.index.isin(failure_idx["index"].unique())
        else:
            raise SettlementValidationError(
                f"Could not map validation failures to rows: {failure_idx.head()!r}"
            ) from exc
        return df[~invalid_mask], df[invalid_mask]


def validate_latest_settlement(
    project_root: str | None = None, date_str: str | None = None
) -> None:
    root = project_root or os.environ.get("PROJECT_ROOT", os.getcwd())
    data_dir = os.path.join(root, "data")
    if date_str:
        file_pattern = f"settlement_{date_str.replace('-', '')}.csv"
        files = glob.glob(os.path.join(data_dir, file_pattern))
    else:
        files = glob.glob(os.path.join(data_dir, "settlement_*.csv"))
    if not files:
        raise FileNotFoundError(f"No settlement file found in {data_dir}")

    latest_file = max(files, key=os.path.getmtime)
    logger.info(f"Validating {latest_file} with Pandera...")

    df = pd.read_csv(latest_file)
    if df.empty:
        raise SettlementValidationError(f"Settlement file is empty: {latest_file}")

    _, invalid = validate_and_quarantine(df, settlement_schema)

    quarantine_rate = len(invalid) / len(df) * 100 if len(df) > 0 else 0
    logger.info(f"Quarantine rate: {quarantine_rate:.1f}% ({len(invalid)}/{len(df)} rows)")

    if not invalid.empty:
        base, ext = os.path.splitext(latest_file)
        if "settlement_" in os.path.basename(base):
            invalid_path = base.replace("settlement_", "quarantine_") + ext
        else:
            invalid_path = base + "_quarantine" + ext
        invalid.to_csv(invalid_path, index=False)
        logger.warning(f"Wrote {len(invalid)} quarantined rows to {invalid_path}")
        raise SettlementValidationError(
            f"Data contract validation failed: {len(invalid)} rows quarantined"
        )

    logger.info("SUCCESS: Data Contract Validated successfully!")


if __name__ == "__main__":
    validate_latest_settlement()
