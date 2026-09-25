import glob
import logging
import os
import re
import time

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

from src.validation.settlement_schema import settlement_schema

logger = logging.getLogger(__name__)

_VOLUME_DROP_WARN = 0.5  # warn if this batch has <50% of the previous curated rows
_STALE_FILE_HOURS = 48  # warn if the settlement file itself is older than this


def _warn_volume_shift(data_dir: str, latest_file: str, n_rows: int, strict: bool = False) -> None:
    """Volume guard: compare against the previous curated batch.

    Catches a truncated/partial PG drop before MERGE bakes it in. Warn-only by
    default; strict (prod) raises so a truncated drop can never merge green.
    Never raises on missing history or unreadable files — only on a measured
    breach.
    """
    try:
        current = "curated_" + os.path.basename(latest_file)
        cands = [
            p
            for p in glob.glob(os.path.join(data_dir, "curated_settlement_*.csv"))
            if os.path.basename(p) != current
        ]
        if not cands:
            return
        prev = max(cands, key=os.path.getmtime)
        with open(prev, encoding="utf-8") as fh:
            prev_n = sum(1 for _ in fh) - 1  # header
        if prev_n > 0 and n_rows < _VOLUME_DROP_WARN * prev_n:
            msg = (
                f"volume shift: {n_rows} rows vs {prev_n} in {os.path.basename(prev)} "
                f"(>{(1 - _VOLUME_DROP_WARN):.0%} drop) — verify the PG drop is complete"
            )
            if strict:
                raise SettlementValidationError(msg)
            logger.warning(msg)
        age_h = (time.time() - os.path.getmtime(latest_file)) / 3600
        if age_h > _STALE_FILE_HOURS:
            msg = (
                f"stale settlement file: {os.path.basename(latest_file)} is {age_h:.1f}h old "
                f"(>{_STALE_FILE_HOURS}h) — verify the PG drop schedule"
            )
            if strict:
                raise SettlementValidationError(msg)
            logger.warning(msg)
    except OSError as exc:
        logger.warning(f"volume check skipped: {exc}")


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


def _validate_large_file(path: str, normalize_fn, chunksize: int = 100_000) -> tuple:
    """Stream a large settlement CSV in chunks; returns (canonical_df, pg_name)."""
    import pandas as _pd

    _parts, _pg = [], "generic"
    for _chunk in _pd.read_csv(
        path, dtype=str, sep=None, engine="python", keep_default_na=False, chunksize=chunksize
    ):
        _chunk = _chunk.replace(r"^\s*$", _pd.NA, regex=True)
        _norm, _pg = normalize_fn(_chunk)
        _parts.append(_norm)
    return (_pd.concat(_parts, ignore_index=True) if _parts else _pd.DataFrame(), _pg)


def validate_latest_settlement(
    project_root: str | None = None, date_str: str | None = None
) -> str | None:
    """Validate latest settlement file, persist normalized valid rows.

    Returns path to curated CSV (normalized, canonical) for reconcile to read,
    so PG INR->paise / date / instrument normalization is not lost.
    """
    root = project_root or os.environ.get("PROJECT_ROOT", os.getcwd())
    data_dir = os.path.join(root, "data")
    if date_str:
        # Same allowlist as reconcile._settlement_source: digit-only
        # YYYY-MM-DD or YYYYMMDD. Anything else (.., /, glob chars) is
        # rejected so date_str can never escape data_dir via the glob.
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}|\d{8}", date_str):
            raise ValueError(f"Bad date_str (want YYYY-MM-DD): {date_str!r}")
        file_pattern = f"settlement_{date_str.replace('-', '')}.csv"
        files = glob.glob(os.path.join(data_dir, file_pattern))
    else:
        files = glob.glob(os.path.join(data_dir, "settlement_*.csv"))
    if not files:
        raise FileNotFoundError(f"No settlement file found in {data_dir}")

    latest_file = max(files, key=os.path.getmtime)
    logger.info(f"Validating {latest_file} with Pandera...")

    # Try PG adapter normalization (Razorpay/Cashfree/PayU/generic) before validation.
    # Generic files pass through unchanged; PG files are converted INR->paise etc.
    # Large files (>256MB) stream in 100k-row chunks so the driver never holds
    # the whole file + validated copies in memory at once.
    _chunk_bytes = 256 * 1024 * 1024
    try:
        _big = os.path.getsize(latest_file) > _chunk_bytes
    except OSError:
        _big = False
    try:
        from src.adapters.settlement import load_settlement_csv, normalize_settlement_df

        if _big:
            df, pg_name = _validate_large_file(latest_file, normalize_settlement_df)
            logger.info(f"Settlement adapter (chunked) -> {len(df)} rows normalized")
        else:
            df, pg_name = load_settlement_csv(latest_file)
            logger.info(f"Settlement adapter detected: {pg_name} -> {len(df)} rows normalized")
    except Exception as exc:
        # Fail closed: raw fallback would lose PG INR->paise/date/instrument
        # normalization and mis-price the MERGE. Fix the adapter, not the data.
        raise SettlementValidationError(
            f"Adapter normalization failed for {latest_file}: {exc}"
        ) from exc
    if df.empty:
        raise SettlementValidationError(f"Settlement file is empty: {latest_file}")

    # No-PAN gate before anything else touches the rows: one card number
    # puts the whole lake in PCI scope. Last-4 + issuer only, ever.
    from src.validation.pan_guard import scan_frame

    pan_hits = scan_frame(df)
    if pan_hits:
        raise SettlementValidationError(
            f"PAN detected in {latest_file} {pan_hits}: refusing batch (store last-4 + issuer only)"
        )

    _, invalid = validate_and_quarantine(df, settlement_schema)
    valid = df.drop(invalid.index) if not invalid.empty else df

    # Persist normalized valid rows for reconcile (PG normalization survives).
    # curated_ prefix avoids matching settlement_*.csv glob on next run.
    curated_path = os.path.join(data_dir, "curated_" + os.path.basename(latest_file))
    if not valid.empty:
        valid.to_csv(curated_path, index=False)
        logger.info(f"Wrote {len(valid)} normalized rows to {curated_path}")

    quarantine_rate = len(invalid) / len(df) * 100 if len(df) > 0 else 0
    logger.info(f"Quarantine rate: {quarantine_rate:.1f}% ({len(invalid)}/{len(df)} rows)")
    from src.common.settings import get_settings

    _warn_volume_shift(data_dir, latest_file, len(valid), strict=get_settings().strict_slo)

    if not invalid.empty:
        base, ext = os.path.splitext(latest_file)
        stem = os.path.basename(base)
        invalid_path = (
            base.replace("settlement_", "quarantine_") + ext
            if "settlement_" in stem
            else base + "_quarantine" + ext
        )
        invalid.to_csv(invalid_path, index=False)
        logger.warning(f"Wrote {len(invalid)} quarantined rows to {invalid_path}")
        raise SettlementValidationError(
            f"Data contract validation failed: {len(invalid)} rows quarantined"
        )

    logger.info("SUCCESS: Data Contract Validated successfully!")
    # Record only on curated-write success: a quarantined/failed batch must
    # never look processed to the redelivery guard.
    from src.validation.file_registry import record_file

    record_file(data_dir, latest_file, date_str)
    return curated_path


if __name__ == "__main__":
    validate_latest_settlement()
