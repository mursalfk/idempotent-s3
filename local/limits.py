# local/limits.py
"""
The one case the robust pattern does NOT close.

robust_handler (claim -> charge -> commit) stops silent drops and repeat work after a
commit. But if the charge LANDS and the function then dies before it can mark the
record done, the retry finds in_progress, assumes the work never finished, and charges
again. No bookkeeping on our side can close that window, because the charge happens on
someone else's side of it. The full cure is to pass the same idempotency key to
whatever takes the money, so the provider recognises the second request as a repeat.

Run:  python -m local.limits
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from local.scaffold import s3_event, FakeTable
from pipeline.handlers import ACCOUNT_PK, COUNTER, _dedupe_key_a, _obj


def robust_blip_after_charge(event, table, attempt):
    """Same shape as robust_handler, but the blip lands after the charge, before commit."""
    idem_key = _dedupe_key_a(_obj(event))
    existing = table.get(idem_key)
    if existing and existing.get("status") == "done":
        return
    if not existing:
        table.put_if_absent(idem_key, {"status": "in_progress"})
    table.increment_counter(ACCOUNT_PK, COUNTER)          # the charge lands
    if attempt == 0:
        raise RuntimeError("blip after the charge, before the commit")
    table.items[idem_key]["status"] = "done"


def main(retries=2):
    table, event = FakeTable(), s3_event()
    for attempt in range(retries + 1):
        try:
            robust_blip_after_charge(event, table, attempt)
            break
        except RuntimeError:
            continue
    charges = table.get(ACCOUNT_PK)[COUNTER]
    print("\n  robust pattern, blip AFTER the charge and BEFORE the commit")
    print(f"  charge count: {charges}   ({'double charged' if charges > 1 else 'charged once'})")
    print("  fix: send the same idempotency key to the system that takes the money\n")


if __name__ == "__main__":
    main()