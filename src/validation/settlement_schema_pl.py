import pandera.polars as pa
import polars as pl
from pandera import Check

from src.common.schemas import INSTRUMENT_TYPES

settlement_schema_pl = pa.DataFrameSchema(
    {
        "transaction_id": pa.Column(
            str,
            unique=True,
            nullable=False,
            checks=[Check.str_matches(r"^tx_[a-f0-9]{12}$")],
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
    },
    strict=False,
)
