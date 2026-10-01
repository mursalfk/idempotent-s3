"""
Real Lambda handler for the deploy path. Uses the robust claim -> charge -> commit
pattern against DynamoDB conditional writes. Set HANDLER_MODE to naive, safe, or
robust to demo each behaviour against the live pipeline.

Forcing the retry duplicate on real Lambda:
  FAIL_ONCE=true makes the FIRST invocation raise after the charge so Lambda's
  async retry fires. The "have I already failed once" flag is stored in DynamoDB,
  not in memory, because Lambda reuses warm containers and an in-memory flag would
  be seen by the retry and skip the failure (that was a real bug we hit). The flag
  lives in the table so it survives across invocations and containers.
"""
import hashlib
import os

import boto3

REGION = os.environ.get("AWS_REGION", "eu-west-3")
TABLE = os.environ.get("STATE_TABLE", "idempotent-s3-state")
MODE = os.environ.get("HANDLER_MODE", "robust")
FAIL_ONCE = os.environ.get("FAIL_ONCE", "false").lower() == "true"

_dynamo = boto3.resource("dynamodb", region_name=REGION)
_ccf = _dynamo.meta.client.exceptions.ConditionalCheckFailedException

ACCOUNT_PK = "account#acme"


def _obj(event):
    return event["Records"][0]["s3"]["object"]


def _key_a(o):
    return f"idem#{o['key']}#{o.get('versionId', 'null')}"


def _key_b(o):
    raw = f"{o['key']}|{o.get('eTag')}|{o.get('sequencer')}".encode("utf-8")
    return "idem#" + hashlib.sha256(raw).hexdigest()[:16]


def _charge(table):
    table.update_item(
        Key={"pk": ACCOUNT_PK},
        UpdateExpression="ADD charges :one",
        ExpressionAttributeValues={":one": 1},
    )


def _maybe_fail(table, o):
    """Fail once, tracked in DynamoDB so the retry (warm container) still fails."""
    if not FAIL_ONCE:
        return
    flag = f"failflag#{o['key']}"
    try:
        table.put_item(Item={"pk": flag}, ConditionExpression="attribute_not_exists(pk)")
    except _ccf:
        return  # the flag already exists: this is the retry, let it succeed
    raise RuntimeError("forced first-attempt failure; Lambda will retry")


def lambda_handler(event, context):
    table = _dynamo.Table(TABLE)
    o = _obj(event)

    if MODE == "naive":
        _charge(table)
        _maybe_fail(table, o)
        return {"mode": MODE, "charged": True}

    idem_key = _key_a(o)

    if MODE == "robust":
        existing = table.get_item(Key={"pk": idem_key}).get("Item")
        if existing and existing.get("status") == "done":
            return {"mode": MODE, "deduped": True}
        if not existing:
            try:
                table.put_item(
                    Item={"pk": idem_key, "status": "in_progress"},
                    ConditionExpression="attribute_not_exists(pk)",
                )
            except _ccf:
                return {"mode": MODE, "deduped": True}
        _maybe_fail(table, o)  # if this raises, record stays in_progress for retry/DLQ
        _charge(table)
        table.update_item(
            Key={"pk": idem_key},
            UpdateExpression="SET #s = :done",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":done": "done"},
        )
        return {"mode": MODE, "charged": True}

    # safe (claim-first)
    try:
        table.put_item(
            Item={"pk": idem_key, "seen": True},
            ConditionExpression="attribute_not_exists(pk)",
        )
    except _ccf:
        return {"mode": MODE, "deduped": True}
    _charge(table)
    _maybe_fail(table, o)
    return {"mode": MODE, "charged": True}