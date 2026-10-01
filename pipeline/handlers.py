"""
Three handlers over the same S3 event, each fixing a gap in the one before it.

naive_handler   does the side effect (charge), then raises before returning.
                Lambda's async retry re-invokes it and the charge runs again.
                One upload, count = 2.

safe_handler_a  idempotency key = object key + versionId. Claims the key with a
                conditional write BEFORE charging, so a retry is skipped. Never
                double-charges. Needs bucket versioning. Trade-off: if the charge
                fails after the claim, the retry is deduped and the charge is
                silently dropped.

safe_handler_b  same guard, key = hash of key + eTag + sequencer. No versioning
                required. Same claim-first trade-off.

robust_handler  claim -> charge -> mark complete. The record is marked done only
                AFTER the charge succeeds. A retry that finds an in_progress record
                re-attempts; one that finds a done record skips. Repeated real
                failures fall to the DLQ instead of being silently deduped. Never
                double-charges and never silently drops.
"""
import hashlib

from local.scaffold import ConditionalCheckFailed

ACCOUNT_PK = "account#acme"
COUNTER = "charges"


def _obj(event):
    return event["Records"][0]["s3"]["object"]


def naive_handler(event, table, fail_after_side_effect=False):
    o = _obj(event)
    count = table.increment_counter(ACCOUNT_PK, COUNTER)
    if fail_after_side_effect:
        raise RuntimeError("downstream failed after the charge; Lambda will retry")
    return {"charged": True, "count": count, "key": o["key"]}


def _dedupe_key_a(o):
    return f"idem#{o['key']}#{o['versionId']}"


def _dedupe_key_b(o):
    raw = f"{o['key']}|{o['eTag']}|{o['sequencer']}".encode("utf-8")
    return "idem#" + hashlib.sha256(raw).hexdigest()[:16]


def _safe(event, table, key_fn, fail_after_side_effect=False):
    o = _obj(event)
    idem_key = key_fn(o)
    try:
        table.put_if_absent(idem_key, {"seen": True})
    except ConditionalCheckFailed:
        return {"charged": False, "deduped": True, "count": table.get(ACCOUNT_PK)[COUNTER]}
    count = table.increment_counter(ACCOUNT_PK, COUNTER)
    if fail_after_side_effect:
        raise RuntimeError("downstream failed after the charge; Lambda will retry")
    return {"charged": True, "count": count, "key": o["key"]}


def safe_handler_a(event, table, fail_after_side_effect=False):
    return _safe(event, table, _dedupe_key_a, fail_after_side_effect)


def safe_handler_b(event, table, fail_after_side_effect=False):
    return _safe(event, table, _dedupe_key_b, fail_after_side_effect)


def robust_handler(event, table, fail_after_side_effect=False):
    o = _obj(event)
    idem_key = _dedupe_key_a(o)

    existing = table.get(idem_key)
    if existing and existing.get("status") == "done":
        return {"charged": False, "deduped": True, "count": table.get(ACCOUNT_PK)[COUNTER]}

    if not existing:
        try:
            table.put_if_absent(idem_key, {"status": "in_progress"})
        except ConditionalCheckFailed:
            return {"charged": False, "deduped": True, "count": table.get(ACCOUNT_PK)[COUNTER]}

    if fail_after_side_effect:
        raise RuntimeError("charge failed; record left in_progress for retry / DLQ")

    count = table.increment_counter(ACCOUNT_PK, COUNTER)
    table.items[idem_key]["status"] = "done"
    return {"charged": True, "count": count, "key": o["key"]}
