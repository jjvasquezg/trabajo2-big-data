"""Windowing consumer for the FINSIR transaction stream.

Reads the Kinesis stream with a one-minute tumbling window and computes, per
category, the event count and the average amount. It also:

  * detects duplicate events by event_id and counts each unique event once,
  * sets invalid events aside with the reason they were discarded,
  * writes one result item per (window, category) to DynamoDB,
  * keeps global counters so the totals reconcile:
        published == unique_processed + duplicates_discarded + invalid_discarded

Deduplication is authoritative: a conditional write on a DynamoDB table with the
event_id as key is atomic, so an event seen in any earlier window (or batch) is
counted once even across invocations. The tumbling-window state only carries the
running aggregates within the current minute.

Environment variables (set by Terraform):
    RESULTS_TABLE    per-window per-category aggregates
    DEDUP_TABLE      seen event_ids (unique counter, dedup guard)
    REJECTS_TABLE    invalid events with their reason
    COUNTERS_TABLE   global reconciliation counters
    AMOUNT_MIN       lower bound for a valid amount
    AMOUNT_MAX       upper bound for a valid amount
    DEDUP_TTL_DAYS   TTL for dedup/reject rows, to keep the tables small
"""

import base64
import json
import os
import time
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")

RESULTS_TABLE = os.environ["RESULTS_TABLE"]
DEDUP_TABLE = os.environ["DEDUP_TABLE"]
REJECTS_TABLE = os.environ["REJECTS_TABLE"]
COUNTERS_TABLE = os.environ["COUNTERS_TABLE"]
AMOUNT_MIN = float(os.environ.get("AMOUNT_MIN", "0"))
AMOUNT_MAX = float(os.environ.get("AMOUNT_MAX", "50000"))
DEDUP_TTL_DAYS = int(os.environ.get("DEDUP_TTL_DAYS", "2"))

results_table = dynamodb.Table(RESULTS_TABLE)
dedup_table = dynamodb.Table(DEDUP_TABLE)
rejects_table = dynamodb.Table(REJECTS_TABLE)
counters_table = dynamodb.Table(COUNTERS_TABLE)


def _validate(event):
    """Return the reason the event is invalid, or None if it is valid."""
    if not isinstance(event, dict):
        return "not_a_json_object"
    if "event_id" not in event:
        return "missing_event_id"
    if "category" not in event or event["category"] in (None, ""):
        return "missing_category"
    amount = event.get("amount")
    if not isinstance(amount, (int, float)) or isinstance(amount, bool):
        return "non_numeric_amount"
    if amount < AMOUNT_MIN or amount > AMOUNT_MAX:
        return "amount_out_of_range"
    return None


def _is_new_event(event_id):
    """Register event_id atomically. True if unseen, False if a duplicate."""
    try:
        dedup_table.put_item(
            Item={
                "event_id": event_id,
                "expires_at": int(time.time()) + DEDUP_TTL_DAYS * 86400,
            },
            ConditionExpression="attribute_not_exists(event_id)",
        )
        return True
    except ClientError as error:
        if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def _bump_counter(name, amount=1):
    """Atomically add to a global reconciliation counter."""
    counters_table.update_item(
        Key={"counter": name},
        UpdateExpression="ADD #v :inc",
        ExpressionAttributeNames={"#v": "value"},
        ExpressionAttributeValues={":inc": Decimal(amount)},
    )


def _reject(event, reason, window_start):
    """Set an invalid event aside with the reason it was discarded."""
    event_id = event.get("event_id", f"no-id-{time.time_ns()}")
    rejects_table.put_item(
        Item={
            "event_id": str(event_id),
            "reason": reason,
            "window_start": window_start,
            "raw": json.dumps(event)[:1000],
            "expires_at": int(time.time()) + DEDUP_TTL_DAYS * 86400,
        }
    )


def _empty_state():
    return {"by_category": {}}


def handler(event, context):
    """Tumbling-window handler.

    Lambda invokes this repeatedly within a one-minute window, passing the
    previous aggregate back in event["state"]. The final invocation of the
    window has event["isFinalInvokeForWindow"] == True; only then do we persist
    the per-category results to DynamoDB.
    """
    window_start = event.get("window", {}).get("start", "unknown")
    state = event.get("state") or _empty_state()
    by_category = state["by_category"]

    for record in event.get("Records", []):
        raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8").strip()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            _reject({"raw": raw}, "invalid_json", window_start)
            _bump_counter("invalid_discarded")
            continue

        reason = _validate(payload)
        if reason is not None:
            _reject(payload, reason, window_start)
            _bump_counter("invalid_discarded")
            continue

        if not _is_new_event(str(payload["event_id"])):
            _bump_counter("duplicates_discarded")
            continue

        # Unique, valid event: count it and fold it into the window aggregate.
        _bump_counter("unique_processed")
        category = payload["category"]
        bucket = by_category.setdefault(category, {"count": 0, "sum": 0.0})
        bucket["count"] += 1
        bucket["sum"] += float(payload["amount"])

    if not event.get("isFinalInvokeForWindow", False):
        # Carry the running aggregate to the next invocation of this window.
        return {"state": {"by_category": by_category}}

    # Window closed: persist one result item per category.
    for category, agg in by_category.items():
        count = agg["count"]
        average = agg["sum"] / count if count else 0.0
        results_table.put_item(
            Item={
                "window_start": window_start,
                "category": category,
                "event_count": count,
                "avg_amount": Decimal(str(round(average, 2))),
                "sum_amount": Decimal(str(round(agg["sum"], 2))),
            }
        )

    return {}
