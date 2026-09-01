# ADR-005: Configurable Fee Engine with Rate Cards

## Status
Accepted

## Context
Payment gateway reconciliation requires verifying that the correct Merchant Discount Rate (MDR) was applied to each transaction. Different payment methods (UPI, credit card, debit card, net banking, wallets, international) have different fee structures. Some merchants may have negotiated custom rates. The fee engine must:

1. Support instrument-type-specific rates
2. Apply GST on MDR where applicable
3. Allow per-merchant overrides
4. Be configurable without code changes
5. Use integer arithmetic to avoid floating-point errors

## Decision
We use a YAML-driven fee engine (`src/processing/fee_engine.py`) with the following design:

- **Rate cards** stored in `config/fee_rates.yaml` — instrument-specific base rates with GST percentage
- **Per-merchant overrides** supported in the YAML under `merchant_overrides`
- **Integer arithmetic** — all amounts in paise, rates in basis points (1 bps = 0.01%), GST multiplied by 100 then divided
- **Tolerance** — configurable tolerance for rounding differences (default: 1 paise)
- **Fallback** — unknown instrument types use a configurable default rate

### Fee Calculation Formula
```
fee_paise = amount_paise * rate_bps // 10000
gst_paise = fee_paise * gst_pct // 100
total_fee_paise = fee_paise + gst_paise
net_paise = amount_paise - total_fee_paise
```

All division uses integer floor division (`//`) — no floating point involved.

## Consequences
- Changing rates requires editing YAML, no code deployment
- Adding a new instrument type requires one YAML entry
- Per-merchant overrides are explicit in config, not inferred
- Integer math means some rounding error exists (e.g., 1 paise off on small amounts) — this is acceptable given the tolerance parameter
- The engine is separate from the reconciliation MERGE logic — it can be tested independently
