import pandas as pd
import pandera.pandas as pa
from pandera import Check

from src.common.schemas import INSTRUMENT_TYPES

_settlement_date_valid = Check(
    lambda s: pd.to_datetime(s, format="%Y-%m-%d", errors="coerce").notna().all(),
    error="settlement_date must be a real calendar date YYYY-MM-DD",
)

settlement_schema = pa.DataFrameSchema(
    {
        "transaction_id": pa.Column(
            str,
            unique=True,
            nullable=False,
            checks=[Check.str_matches(r"^tx_[a-f0-9]{12}$")],
        ),
        "settled_amount_paise": pa.Column(int, Check.gt(0), nullable=False),
        "bank_ref_id": pa.Column(str, nullable=False, unique=True),
        "settlement_date": pa.Column(
            str,
            nullable=False,
            checks=[Check.str_matches(r"^\d{4}-\d{2}-\d{2}$"), _settlement_date_valid],
        ),
        "instrument_type": pa.Column(
            str,
            nullable=False,
            checks=Check.isin(INSTRUMENT_TYPES),
        ),
        # Optional canonical columns — nullable, allow PG adapters to populate.
        "merchant_id": pa.Column(str, nullable=True, required=False),
        "fee_paise": pa.Column(pd.Int64Dtype(), nullable=True, required=False, coerce=True),
        "gst_paise": pa.Column(pd.Int64Dtype(), nullable=True, required=False, coerce=True),
        "settlement_id": pa.Column(str, nullable=True, required=False),
        "utr": pa.Column(str, nullable=True, required=False),
        "currency": pa.Column(str, nullable=True, required=False),
        "gross_amount_paise": pa.Column(
            pd.Int64Dtype(), nullable=True, required=False, coerce=True
        ),
    },
    strict=False,
    coerce=True,
)
