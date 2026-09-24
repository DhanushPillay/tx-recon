"""Provider settlement behavior: batch identity, lag windows, per-provider SLA.

Calendar-date joins create false MISSINGs (cross-midnight batches, holidays,
multi-day cycles). Every settlement belongs to a provider batch
`{provider}:{batch_date}`; webhooks match when inside the lag window.
"""

import logging
import os

import yaml

logger = logging.getLogger(__name__)


def load_providers(path: str | None = None) -> dict:
    """Load providers.yaml. Missing file fails closed (no silent defaults)."""
    from src.common.settings import get_settings

    if path is None:
        settings = get_settings()
        path = os.path.join(settings.project_root, "config", "providers.yaml")
    if not os.path.exists(path):
        raise FileNotFoundError(f"provider config not found: {path}")
    with open(path, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    if not isinstance(cfg, dict) or "providers" not in cfg:
        raise ValueError(f"provider config malformed (need 'providers'): {path}")
    return cfg


def provider_spec(cfg: dict, provider: str | None) -> dict:
    """Provider section merged over defaults; unknown providers get defaults."""
    base = dict(cfg.get("default", {}))
    if provider:
        override = (cfg.get("providers") or {}).get(provider, {})
        if isinstance(override, dict):
            base.update(override)
    base.setdefault("lag_days", 2)
    base.setdefault("late_sla_days", 7)
    return base


def assign_batch(provider: str | None, settlement_date: str) -> str:
    """Stable batch identity `{provider}:{date}` (provider defaults to gateway)."""
    return f"{provider or 'gateway'}:{settlement_date}"


def within_lag(webhook_date: str, settlement_date: str, lag_days: int) -> bool:
    """True when the webhook falls inside the provider's on-time window."""
    from datetime import date as _date

    try:
        w = _date.fromisoformat(str(webhook_date)[:10])
        s = _date.fromisoformat(str(settlement_date)[:10])
    except ValueError:
        return False
    delta = (s - w).days
    return 0 <= delta <= int(lag_days)
