import pandera.pandas as pa
from pandera import Check

from src.common.schemas import INSTRUMENT_TYPES

_settlement_date_valid = Check(
    lambda s: __import__("pandas").to_datetime(s, format="%Y-%m-%d", errors="coerce").notna().all(),
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
    },
    strict=True,
)
