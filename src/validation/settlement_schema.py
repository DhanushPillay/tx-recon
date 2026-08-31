import pandera.pandas as pa

settlement_schema = pa.DataFrameSchema(
    {
        "transaction_id": pa.Column(str, unique=True, nullable=False),
        "settled_amount_paise": pa.Column(int, pa.Check.gt(0), nullable=False),
        "bank_ref_id": pa.Column(str, nullable=False),
    },
    strict=False,
)


if __name__ == "__main__":
    settlement_schema.to_yaml("src/validation/settlement_schema.yaml")
    print("Schema exported to src/validation/settlement_schema.yaml")
