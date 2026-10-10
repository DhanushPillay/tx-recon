import glob
import logging
import os
import re
import time
from datetime import date

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

from src.validation.settlement_schema import bad_calendar_date_mask, settlement_schema

logger = logging.getLogger(__name__)

_VOLUME_DROP_WARN = 0.5
_VOLUME_SPIKE_WARN = 2.0
_STALE_FILE_HOURS = 48


def _settlement_date(path: str) -> date | None:
    m = re.search(r"(\d{4})(\d{2})(\d{2})", os.path.basename(path))
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _warn_volume_shift(data_dir: str, latest_file: str, n_rows: int, strict: bool = False) -> None:

    def _flag(msg: str) -> None:
        if strict:
            raise SettlementValidationError(msg)
        logger.warning(msg)

    try:
        drop_date = _settlement_date(latest_file)
        age_h = (
            (date.today() - drop_date).days * 24
            if drop_date
            else (time.time() - os.path.getmtime(latest_file)) / 3600
        )
        if age_h > _STALE_FILE_HOURS:
            logger.warning(
                f"stale settlement file: {os.path.basename(latest_file)} covers "
                f"{drop_date or 'an unknown date'} and is {age_h / 24:.1f}d old "
                f"(>{_STALE_FILE_HOURS / 24:.0f}d) — verify the PG drop schedule"
            )

        current = "curated_" + os.path.basename(latest_file)
        cands = [
            p
            for p in glob.glob(os.path.join(data_dir, "curated_settlement_*.csv"))
            if os.path.basename(p) != current or os.path.getmtime(p) < os.path.getmtime(latest_file)
        ]
        if not cands:
            return
        prev = max(cands, key=lambda p: (_settlement_date(p) or date.min, os.path.getmtime(p)))
        with open(prev, encoding="utf-8") as fh:
            prev_n = sum(1 for _ in fh) - 1
        if prev_n > 0 and (
            n_rows < _VOLUME_DROP_WARN * prev_n or n_rows > _VOLUME_SPIKE_WARN * prev_n
        ):
            _flag(
                f"volume shift: {n_rows} rows vs {prev_n} in {os.path.basename(prev)} "
                f"(outside [{_VOLUME_DROP_WARN:.0%}, {_VOLUME_SPIKE_WARN:.0%}]) — "
                "verify the PG drop is complete"
            )
    except OSError as exc:
        logger.warning(f"volume check skipped: {exc}")


class SettlementValidationError(Exception):
    pass


def validate_and_quarantine(
    df: pd.DataFrame, schema: pa.DataFrameSchema
) -> tuple[pd.DataFrame, pd.DataFrame]:
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
    import pandas as _pd

    from src.adapters.settlement import _CSV_NA_VALUES, blank_to_na, sniff_csv_delimiter

    _parts, _pg = [], "generic"
    for _chunk in _pd.read_csv(
        path,
        dtype=str,
        sep=sniff_csv_delimiter(path),
        engine="c",
        keep_default_na=False,
        na_values=_CSV_NA_VALUES,
        chunksize=chunksize,
    ):
        _chunk = blank_to_na(_chunk)
        _norm, _pg = normalize_fn(_chunk)
        _parts.append(_norm)
    return (_pd.concat(_parts, ignore_index=True) if _parts else _pd.DataFrame(), _pg)


def validate_latest_settlement(
    project_root: str | None = None, date_str: str | None = None
) -> str | None:
    root = project_root or os.environ.get("PROJECT_ROOT", os.getcwd())
    data_dir = os.path.join(root, "data")
    if date_str:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}|\d{8}", date_str):
            raise ValueError(f"Bad date_str (want YYYY-MM-DD): {date_str!r}")
        file_pattern = f"settlement_{date_str.replace('-', '')}.csv"
        files = glob.glob(os.path.join(data_dir, file_pattern))
    else:
        files = glob.glob(os.path.join(data_dir, "settlement_*.csv"))
    if not files:
        raise FileNotFoundError(f"No settlement file found in {data_dir}")

    latest_file = max(files, key=lambda p: (_settlement_date(p) or date.min, os.path.getmtime(p)))
    logger.info(f"Validating {latest_file} with Pandera...")

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
        raise SettlementValidationError(
            f"Adapter normalization failed for {latest_file}: {exc}"
        ) from exc
    if df.empty:
        raise SettlementValidationError(f"Settlement file is empty: {latest_file}")

    from src.validation.pan_guard import scan_frame

    pan_hits = scan_frame(df)
    if pan_hits:
        raise SettlementValidationError(
            f"PAN detected in {latest_file} {pan_hits}: refusing batch. "
            "Inspect the flagged columns before acting -- a 13-19 digit run that "
            "is Luhn-valid and starts with a real issuer prefix is almost always "
            "a card number, but confirm against the source export rather than "
            "purging the file blind. Store last-4 + issuer only, ever."
        )

    valid, invalid = validate_and_quarantine(df, settlement_schema)
    impossible_dates = bad_calendar_date_mask(df["settlement_date"])
    if impossible_dates.any():
        quarantined = invalid.index.union(df.index[impossible_dates])
        logger.warning(f"Impossible calendar dates quarantined: {len(quarantined)}")
        valid, invalid = df.drop(index=quarantined), df.loc[quarantined]

    quarantine_rate = len(invalid) / len(df) * 100 if len(df) > 0 else 0
    logger.info(f"Quarantine rate: {quarantine_rate:.1f}% ({len(invalid)}/{len(df)} rows)")
    from src.common.settings import get_settings

    _warn_volume_shift(data_dir, latest_file, len(valid), strict=get_settings().strict_slo)

    try:
        max_pct = float(os.environ.get("QUARANTINE_MAX_PCT", 0))
    except (TypeError, ValueError):
        max_pct = 0
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
        if quarantine_rate > max_pct:
            raise SettlementValidationError(
                f"Data contract validation failed: {len(invalid)} rows quarantined "
                f"({quarantine_rate:.1f}% > {max_pct:.1f}% allowed)"
            )
        logger.warning(
            f"Proceeding with {len(valid)} valid rows; {len(invalid)} quarantined within allowance"
        )

    # only write curated after quarantine passes, so a bad batch leaves nothing behind
    curated_path = os.path.join(data_dir, "curated_" + os.path.basename(latest_file))
    _tmp = curated_path + ".tmp"
    valid.to_csv(_tmp, index=False)
    os.replace(_tmp, curated_path)
    logger.info(f"Wrote {len(valid)} normalized rows to {curated_path}")

    logger.info("SUCCESS: Data Contract Validated successfully!")
    from src.validation.file_registry import record_file

    record_file(data_dir, latest_file, date_str)
    return curated_path


if __name__ == "__main__":
    validate_latest_settlement()
