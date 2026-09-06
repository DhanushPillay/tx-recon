# tx-recon, explained for non-technical readers

## The problem, in one sentence

When you pay a shop online, two records are created — the payment app's
instant notification and the bank's next-day statement — and someone has to
check that they agree.

## Why that checking matters

Every online payment carries a small fee (called MDR) plus tax on that fee.
The bank's statement arrives a day late, and its fee math doesn't always match
the payment app's. Today, finance teams check this by hand in spreadsheets.
That is slow (month-end closing drags on for days) and it hides lost money:
if the bank takes a slightly bigger fee than it should, nobody notices.

## What this project does

tx-recon is a small automated system that does the matching for them:

1. **Listens.** Payment notifications arrive in real time and are collected
   in order (like a post office sorting incoming letters).
2. **Receives the bank file.** The next day, the bank's statement arrives as
   a spreadsheet-like file.
3. **Checks the file first.** Before trusting the bank file, the system
   inspects it — correct columns, no duplicates, sensible amounts. Bad rows
   are set aside in a "quarantine" pile for a human to look at, never silently
   thrown away.
4. **Recalculates every fee from scratch.** Using a published rate card (fee
   percentage per payment method + 18% tax), it recomputes what each payment's
   fee should have been — using whole paise only, never decimals, so rounding
   can never corrupt money math.
5. **Matches.** Each payment is compared against its bank record: exact match,
   tiny rounding difference (accepted within 1 paise), fee disagreement, or
   missing on one side. Each outcome is recorded.
6. **Repeats safely.** The whole process can be re-run any number of times
   without double-counting — late corrections from the bank simply update the
   previous answer.

## How it proves it's right

Speed means nothing if the matches are wrong, so correctness is tested first:

- A test rig deliberately plants 7 kinds of problems (wrong fees, rounding
  edges, missing records, duplicates, late corrections…) with a secret answer
  key the matcher never sees.
- Latest result: **every planted problem caught, zero false alarms**
  (precision = recall = F1 = 1.0, 0 false positives).

## How fast is it (measured September 2026, ordinary desktop)

- **Ingestion:** ~142,000 payment notifications per second into the queue,
  typical delay under 1 millisecond.
- **File checking:** 100,000 bank rows checked in ~9–89 milliseconds
  depending on method (fastest: plain checks; ~10x cost for the strict
  contract-based checker — considered worth it since it guards real money).
- **Lake storage reads/writes at scale:** not yet measured on this machine —
  the database connection was broken until recently and is now fixed; full
  scale runs are still pending. No number is claimed here.

## Honest limits

- This is a **demonstration**, not a production system: it runs on one
  computer using sample (fake) data, not real money.
- It does not yet handle billions of transactions or guarantee delivery if a
  machine crashes mid-run.
- Anything above marked "pending" is genuinely not done — the README's
  numbers section is updated only with measurements actually taken.

## Seeing it work

With Docker running: `docker compose up -d` starts the supporting services,
`make demo` runs the 10-second correctness check, and
`python -m src.pipeline --date 2026-09-04` runs the full pipeline on sample
data.
