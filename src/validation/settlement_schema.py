import pandera.pandas as pa
from pandera import Check

settlement_schema = pa.DataFrameSchema(
    {
        "transaction_id": pa.Column(
            str,
            unique=True,
            nullable=False,
            checks=[Check.str_matches(r"^tx_[a-f0-9]{12}$")],
        ),
        "settled_amount_paise": pa.Column(int, Check.gt(0), nullable=False),
        "bank_ref_id": pa.Column(str, nullable=False),
        "settlement_date": pa.Column(str, nullable=False),
        "instrument_type": pa.Column(
            str,
            nullable=False,
            checks=Check.isin(
                ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]
            ),
        ),
    },
    strict=False,
)


if __name__ == "__main__":
    settlement_schema.to_yaml("src/validation/settlement_schema.yaml")
    print("Schema exported to src/validation/settlement_schema.yaml")
