import csv
import os
import random
import uuid
from datetime import datetime, timedelta, timezone


def generate_settlement_file(num_records=100, output_dir="data"):
    headers = [
        "bank_ref_id",
        "transaction_id",
        "settled_amount_paise",
        "settlement_date",
    ]

    # 1.5% MDR
    MDR_RATE = 0.015

    os.makedirs(output_dir, exist_ok=True)
    file_name = f"settlement_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
    output_file = os.path.join(output_dir, file_name)

    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        for _ in range(num_records):
            # Same structure as the webhooks so we have things that match
            tx_id = f"tx_{uuid.uuid4().hex[:12]}"
            gateway_amount = random.randint(1000, 1000000)

            # The bank deducts 1.5% and gives us the net amount
            fee = int(gateway_amount * MDR_RATE)
            net_amount = gateway_amount - fee

            # Simulated settlement date (usually T+1 or T+2)
            settle_date = (datetime.now(timezone.utc) + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )

            writer.writerow(
                [f"bnk_{uuid.uuid4().hex[:8]}", tx_id, net_amount, settle_date]
            )

    print(f"Generated {num_records} settlement records in {output_file}")


if __name__ == "__main__":
    generate_settlement_file(500)
