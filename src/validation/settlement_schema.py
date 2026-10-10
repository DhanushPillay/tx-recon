import pandas as pd
import pandera.pandas as pa
from pandera import Check

from src.common.schemas import INSTRUMENT_TYPES


def bad_calendar_date_mask(series: pd.Series) -> pd.Series:
    return series.notna() & pd.to_datetime(series, format="%Y-%m-%d", errors="coerce").isna()


settlement_schema = pa.DataFrameSchema(
    {
        "transaction_id": pa.Column(
            str,
            unique=False,
            nullable=False,
            checks=[Check.str_matches(r"^[A-Za-z0-9_]{4,64}$")],
        ),
        "settled_amount_paise": pa.Column(int, Check.gt(0), nullable=False),
        "bank_ref_id": pa.Column(str, nullable=False),
        "settlement_date": pa.Column(
            str,
            nullable=False,
            checks=[Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")],
        ),
        "instrument_type": pa.Column(
            str,
            nullable=False,
            checks=Check.isin(INSTRUMENT_TYPES),
        ),
        "merchant_id": pa.Column(str, nullable=True, required=False),
        "fee_paise": pa.Column(
            pd.Int64Dtype(), nullable=True, required=False, coerce=True, checks=Check.ge(0)
        ),
        "gst_paise": pa.Column(
            pd.Int64Dtype(), nullable=True, required=False, coerce=True, checks=Check.ge(0)
        ),
        "settlement_id": pa.Column(str, nullable=True, required=False),
        "utr": pa.Column(str, nullable=True, required=False),
        "currency": pa.Column(
            str,
            nullable=True,
            required=False,
            checks=Check.isin(["INR"]),
        ),
        "gross_amount_paise": pa.Column(
            pd.Int64Dtype(),
            nullable=True,
            required=False,
            coerce=True,
            checks=Check.ge(0),
        ),
        "provider": pa.Column(str, nullable=True, required=False),
    },
    strict=False,
    coerce=True,
)
