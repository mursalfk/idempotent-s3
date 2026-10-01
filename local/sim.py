"""
Local demo. Runs each handler through Lambda's async-retry behaviour and prints
what happens to the charge count. No AWS account needed.

Two scenarios:
  transient : the charge succeeds, then the handler throws once (a downstream blip).
              Lambda retries. We want: charged exactly once, no double charge.
  poison    : the charge itself fails every attempt. Lambda exhausts its retries.
              We want: not charged, and the work is NOT silently swallowed.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from local.scaffold import s3_event, FakeTable
from pipeline.handlers import (
    naive_handler, safe_handler_a, safe_handler_b, robust_handler,
    ACCOUNT_PK, COUNTER,
)

HANDLERS = [
    ("naive", naive_handler),
    ("safe_a  (key + versionId)", safe_handler_a),
    ("safe_b  (key + eTag + seq)", safe_handler_b),
    ("robust  (claim/work/commit)", robust_handler),
]


def run(handler, scenario, retries=2):
    table = FakeTable()
    ev = s3_event()
    completed = False
    for attempt in range(retries + 1):
        try:
            if scenario == "transient":
                handler(ev, table, fail_after_side_effect=(attempt == 0))
            else:  # poison: fails every attempt
                handler(ev, table, fail_after_side_effect=True)
            completed = True
            break
        except Exception:
            continue
    row = table.get(ACCOUNT_PK)
    count = row[COUNTER] if row else 0
    return count, completed


def table_for(scenario, want):
    print(f"\n  scenario: {scenario}   (want: {want})")
    print(f"  {'handler':30} {'charge count':>13}   verdict")
    print(f"  {'-'*30} {'-'*13}   {'-'*30}")
    for name, h in HANDLERS:
        count, _ = run(h, scenario)
        if scenario == "transient":
            verdict = "double charged" if count > 1 else "charged once"
        else:
            verdict = "charge dropped" if count == 0 else f"charged {count}"
        print(f"  {name:30} {count:>13}   {verdict}")


def main():
    print("\n  Idempotent S3: what a retried Lambda does to a charge\n")
    table_for("transient", "charged exactly once")
    table_for("poison", "not silently dropped")
    print()


if __name__ == "__main__":
    main()
