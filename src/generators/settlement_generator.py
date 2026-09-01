import csv
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

from src.common.settings import get_settings

INSTRUMENT_TYPES = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NETBANKING", "WALLET", "INTERNATIONAL"]


def generate_settlement_file(num_records=100, output_dir=None, webhook_ids=None):
    settings = get_settings()
    out = output_dir or os.path.join(settings.project_root, "data")
    os.makedirs(out, exist_ok=True)

    headers = [
        "bank_ref_id",
        "transaction_id",
        "settled_amount_paise",
        "settlement_date",
        "instrument_type",
    ]

    file_name = f"settlement_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
    output_file = os.path.join(out, file_name)

    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        for _ in range(num_records):
            if webhook_ids:
                tx_id = random.choice(webhook_ids)
            else:
                tx_id = f"tx_{uuid.uuid4().hex[:12]}"

            gateway_amount = random.randint(1000, 1000000)
            instrument = random.choice(INSTRUMENT_TYPES)
            fee = int(gateway_amount * settings.default_mdr_rate)
            net_amount = gateway_amount - fee
            settle_date = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")

            writer.writerow(
                [f"bnk_{uuid.uuid4().hex[:8]}", tx_id, net_amount, settle_date, instrument]
            )

    print(f"Generated {num_records} settlement records in {output_file}")
    return output_file


if __name__ == "__main__":
    generate_settlement_file(500)
