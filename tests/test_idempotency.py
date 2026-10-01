import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from local.scaffold import s3_event, FakeTable
from pipeline.handlers import (
    naive_handler, safe_handler_a, safe_handler_b, robust_handler,
    ACCOUNT_PK, COUNTER,
)


def _run(handler, scenario, retries=2):
    table = FakeTable()
    ev = s3_event()
    for attempt in range(retries + 1):
        try:
            fail = (attempt == 0) if scenario == "transient" else True
            handler(ev, table, fail_after_side_effect=fail)
            break
        except Exception:
            continue
    row = table.get(ACCOUNT_PK)
    return row[COUNTER] if row else 0


def test_naive_double_charges_on_transient_failure():
    assert _run(naive_handler, "transient") == 2


def test_safe_a_charges_once_on_transient_failure():
    assert _run(safe_handler_a, "transient") == 1


def test_safe_b_charges_once_on_transient_failure():
    assert _run(safe_handler_b, "transient") == 1


def test_robust_charges_once_on_transient_failure():
    assert _run(robust_handler, "transient") == 1


def test_naive_multi_charges_on_poison():
    # a persistently failing charge gets retried and re-charged every attempt
    assert _run(naive_handler, "poison") == 3


def test_robust_does_not_charge_on_poison():
    # correct outcome: no charge committed, record left for retry / DLQ
    assert _run(robust_handler, "poison") == 0


def test_claim_first_leaves_money_state_inconsistent_on_poison():
    # the subtle trap: claim-first increments before the throw, so a charge lands
    # even though the operation is reported failed. This asserts the inconsistency
    # exists, which is exactly what the robust handler avoids.
    assert _run(safe_handler_a, "poison") == 1
    assert _run(safe_handler_b, "poison") == 1
