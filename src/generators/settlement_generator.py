import csv
import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

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
    planned: list[tuple[str, int, str]] | None = None,
) -> str:
    """Generate a synthetic bank settlement CSV. Fee-accurate: net comes from FeeEngine.

    planned: optional (transaction_id, gateway_amount_paise, instrument_type)
    triples for coordinated seeding — e.g. pipeline --demo inserts webhooks
    first, then builds settlements from the same triples so the demo matches.
    (webhook_ids alone can't match: amounts stay random and unknown to webhooks.)
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
    ]

    settlement_date = date_str or (datetime.now(timezone.utc) + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    file_name = f"settlement_{settlement_date.replace('-', '')}.csv"
    output_file = os.path.join(out, file_name)

    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        items = planned if planned is not None else [None] * num_records
        for item in items:
            if item is not None:
                tx_id, gateway_amount, instrument = item
            else:
                tx_id = (
                    rnd.choice(webhook_ids)
                    if webhook_ids
                    else f"tx_{uuid.UUID(int=rnd.getrandbits(128)).hex[:12]}"
                )
                gateway_amount = rnd.randint(1000, 1000000)
                instrument = rnd.choice(INSTRUMENT_TYPES)

            fee_result = fee_engine.compute_fee(gateway_amount, instrument_type=instrument)
            net_amount = fee_result.net_paise

            bank_ref = f"bnk_{uuid.UUID(int=rnd.getrandbits(128)).hex[:8]}"

            writer.writerow([bank_ref, tx_id, net_amount, settlement_date, instrument])

    logger.info(f"Generated {num_records} settlement records in {output_file}")
    return output_file


if __name__ == "__main__":
    generate_settlement_file(500)
