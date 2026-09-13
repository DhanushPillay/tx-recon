import csv
import logging
import os
import random
import uuid
from datetime import UTC, datetime, timedelta

from src.common.schemas import INSTRUMENT_TYPES
from src.common.settings import get_settings
from src.processing.fee_engine import get_fee_engine

logger = logging.getLogger(__name__)


def generate_settlement_file(
    num_records: int = 100,
    output_dir: str | None = None,
    webhook_ids: list[str] | None = None,
    seed: int | None = None,
    date_str: str | None = None,
    planned: list[tuple[str, int, str]] | list[tuple[str, int, str, str]] | None = None,
    merchant_id: str | None = None,
) -> str:
    """Generate a synthetic bank settlement CSV. Fee-accurate: net comes from FeeEngine.

    planned: optional (transaction_id, gateway_amount_paise, instrument_type)
    triples for coordinated seeding — e.g. pipeline --demo inserts webhooks
    first, then builds settlements from the same triples so the demo matches.
    (webhook_ids alone can't match: amounts stay random and unknown to webhooks.)
    When planned tuples have 4 elements, the 4th is merchant_id.
    """
    if planned is None and num_records <= 0:
        raise ValueError(f"num_records must be positive, got {num_records}")
    settings = get_settings()
    fee_engine = get_fee_engine()
    out = output_dir or os.path.join(settings.project_root, "data")
    os.makedirs(out, exist_ok=True)

    rnd = random.Random(seed) if seed is not None else random

    headers = [
        "bank_ref_id",
        "transaction_id",
        "settled_amount_paise",
        "settlement_date",
        "instrument_type",
        "merchant_id",
    ]

    settlement_date = date_str or (datetime.now(UTC) + timedelta(days=1)).strftime("%Y-%m-%d")
    file_name = f"settlement_{settlement_date.replace('-', '')}.csv"
    output_file = os.path.join(out, file_name)

    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        items = planned if planned is not None else [None] * num_records
        for item in items:
            if item is not None:
                # Support 3-tuple (legacy) or 4-tuple (with merchant_id)
                if len(item) == 4:
                    tx_id, gateway_amount, instrument, merch = item  # type: ignore[misc]
                else:
                    tx_id, gateway_amount, instrument = item  # type: ignore[misc]
                    merch = merchant_id or "UNKNOWN"
            else:
                tx_id = (
                    rnd.choice(webhook_ids)
                    if webhook_ids
                    else f"tx_{uuid.UUID(int=rnd.getrandbits(128)).hex[:12]}"
                )
                gateway_amount = rnd.randint(1000, 1000000)
                instrument = rnd.choice(INSTRUMENT_TYPES)
                merch = merchant_id or rnd.choice(["merch_demo", "UNKNOWN", "merch_12345"])

            fee_result = fee_engine.compute_fee(
                gateway_amount,
                instrument_type=instrument,
                merchant_id=merch if merch != "UNKNOWN" else None,
            )
            net_amount = fee_result.net_paise

            bank_ref = f"bnk_{uuid.UUID(int=rnd.getrandbits(128)).hex[:12]}"

            writer.writerow([bank_ref, tx_id, net_amount, settlement_date, instrument, merch])

    logger.info(f"Generated {num_records} settlement records in {output_file}")
    return output_file


if __name__ == "__main__":
    generate_settlement_file(500)
