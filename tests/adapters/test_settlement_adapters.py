import pandas as pd

from src.adapters.settlement import (
    GenericAdapter,
    PayUAdapter,
    RazorpayAdapter,
    detect_adapter,
    load_settlement_csv,
)


def test_generic_adapter_canonical():
    df = pd.DataFrame(
        {
            "bank_ref_id": ["bnk_aaaaaaaaaaaa"],
            "transaction_id": ["tx_a1b2c3d4e5f6"],
            "settled_amount_paise": ["100000"],
            "settlement_date": ["2024-01-16"],
            "instrument_type": ["CREDIT_CARD"],
            "merchant_id": ["merch_001"],
        }
    )
    tmp = GenericAdapter().normalize(df)
    assert tmp.iloc[0]["transaction_id"] == "tx_a1b2c3d4e5f6"
    assert tmp.iloc[0]["settled_amount_paise"] == 100000
    assert tmp.iloc[0]["merchant_id"] == "merch_001"
    assert tmp.iloc[0]["currency"] == "INR"


def test_razorpay_adapter_inr_conversion_and_mapping():
    # Razorpay ships Amount/Settlement Amount as INR decimal strings with Fee/Tax
    df = pd.DataFrame(
        {
            "Payment Id": ["pay_ABC123"],
            "Settlement Id": ["setl_XYZ"],
            "Settlement UTR": ["UTR123456"],
            "Amount": ["1000.00"],
            "Fee": ["20.00"],
            "Tax": ["3.60"],
            "Settlement Amount": ["976.40"],
            "Settlement Date": ["2024-02-15"],
            "Payment Method": ["card"],
        }
    )
    out = RazorpayAdapter().normalize(df)
    # Razorpay Payment Id preserved as transaction_id (pay_ prefix, not synthetic tx_)
    assert out.iloc[0]["transaction_id"] == "pay_ABC123"
    assert out.iloc[0]["settled_amount_paise"] == 97640
    assert out.iloc[0]["gross_amount_paise"] == 100000
    assert out.iloc[0]["fee_paise"] == 2000
    assert out.iloc[0]["gst_paise"] == 360
    assert out.iloc[0]["instrument_type"] == "CREDIT_CARD"
    assert out.iloc[0]["utr"] == "UTR123456"


def test_cashfree_adapter(tmp_path):
    df = pd.DataFrame(
        {
            "payment_id": ["pay_999"],
            "order_id": ["order_1"],
            "settlement_id": ["stl_1"],
            "utr": ["utr_1"],
            "payment_amount": ["500"],
            "fee": ["5"],
            "tax": ["0.9"],
            "settlement_amount": ["494.10"],
            "settlement_date": ["2025-06-01"],
            "payment_method": ["upi"],
            "merchant_id": ["merch_demo"],
        }
    )
    path = tmp_path / "cashfree.csv"
    df.to_csv(path, index=False)
    canon, pg = load_settlement_csv(str(path))
    assert pg == "cashfree"
    assert canon.iloc[0]["settled_amount_paise"] == 49410
    assert canon.iloc[0]["instrument_type"] == "UPI"


def test_payu_adapter_maps_upi():
    df = pd.DataFrame(
        {
            "mihpayid": ["4039937155"],
            "request_id": ["req1"],
            "amount": ["2000"],
            "surcharge": ["30"],
            "tax": ["5.4"],
            "settlement_amount": ["1964.60"],
            "utr": ["utr_payu"],
            "payment_mode": ["UPI"],
            "settlement_date": ["2025-05-20"],
        }
    )
    out = PayUAdapter().normalize(df)
    assert out.iloc[0]["instrument_type"] == "UPI"
    assert out.iloc[0]["settled_amount_paise"] == 196460


def test_detect_adapter_generic_vs_razorpay():
    generic = pd.DataFrame({"transaction_id": ["tx_a1b2c3d4e5f6"], "bank_ref_id": ["bnk_1"]})
    assert detect_adapter(generic).pg_name == "generic"
    rzp = pd.DataFrame({"Payment Id": ["pay_1"], "Settlement Id": ["setl_1"]})
    assert detect_adapter(rzp).pg_name == "razorpay"


def test_roundtrip_quarantine_path():
    # Empty string becomes NA, not zero
    df = pd.DataFrame(
        {
            "bank_ref_id": ["bnk_1"],
            "transaction_id": ["tx_a1b2c3d4e5f6"],
            "settled_amount_paise": [""],
            "settlement_date": ["not-a-date"],
            "instrument_type": ["CREDIT_CARD"],
        }
    )
    out = GenericAdapter().normalize(df)
    assert pd.isna(out.iloc[0]["settled_amount_paise"])
    assert pd.isna(out.iloc[0]["settlement_date"])


def test_load_settlement_csv_mixed_merchant():
    df = pd.DataFrame(
        {
            "bank_ref_id": ["bnk_aaaaaaaaaaaa", "bnk_bbbbbbbbbbbb"],
            "transaction_id": ["tx_a1b2c3d4e5f6", "tx_b1b2c3d4e5f6"],
            "settled_amount_paise": [100000, 200000],
            "settlement_date": ["2024-01-16", "2025-06-01"],
            "instrument_type": ["CREDIT_CARD", "UPI"],
            "merchant_id": ["merch_001", "UNKNOWN"],
        }
    )
    import pathlib
    import tempfile

    tmp = pathlib.Path(tempfile.mkdtemp())
    p = tmp / "sett.csv"
    df.to_csv(p, index=False)
    canon, _ = load_settlement_csv(str(p))
    assert canon.iloc[0]["merchant_id"] == "merch_001"
    assert canon.iloc[1]["merchant_id"] == "UNKNOWN"
