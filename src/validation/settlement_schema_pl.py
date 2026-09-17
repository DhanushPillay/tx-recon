import pandera.polars as pa
import polars as pl
from pandera import Check

from src.common.schemas import INSTRUMENT_TYPES

settlement_schema_pl = pa.DataFrameSchema(
    {
        "transaction_id": pa.Column(
            str,
            # ponytail: dup detection lives in reconcile dedup (counted), not here.
            unique=False,
            nullable=False,
            checks=[Check.str_matches(r"^[A-Za-z0-9_]{4,64}$")],
        ),
        "settled_amount_paise": pa.Column(pl.Int64, Check.gt(0), nullable=False),
        "bank_ref_id": pa.Column(str, nullable=False),
        "settlement_date": pa.Column(
            str, nullable=False, checks=[Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")]
        ),
        "instrument_type": pa.Column(
            str,
            nullable=False,
            checks=Check.isin(INSTRUMENT_TYPES),
        ),
        "merchant_id": pa.Column(str, nullable=True, required=False),
        "fee_paise": pa.Column(pl.Int64, nullable=True, required=False),
        "gst_paise": pa.Column(pl.Int64, nullable=True, required=False),
        "settlement_id": pa.Column(str, nullable=True, required=False),
        "utr": pa.Column(str, nullable=True, required=False),
        "currency": pa.Column(str, nullable=True, required=False),
        "gross_amount_paise": pa.Column(pl.Int64, nullable=True, required=False),
    },
    strict=False,
    coerce=True,
)
