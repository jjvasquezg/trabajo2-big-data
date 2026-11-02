"""Producer that publishes FINSIR transaction events to a Kinesis Data Stream.

Each event is a financial movement with a product category and a numeric amount.
The producer runs continuously for a configurable number of minutes and, on
purpose, repeats some events (same event_id) and emits invalid ones (missing
field or amount out of range), so Parte B can prove that the counts reconcile.

Usage:
    python producer.py --stream finsir-transacciones --minutes 5 --rate 20

Credentials are read from the environment / default AWS profile; never hardcode
them. The exact count of published, duplicate and invalid events is written to
stdout and to producer_summary.json so it can be checked against DynamoDB.
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

import boto3

# Product categories, aligned with the FINSIR products of Parte A.
CATEGORIES = [
    "nomina",
    "tarjeta_credito",
    "cuenta_corriente",
    "pension",
    "deposito",
    "hipoteca",
]

# An amount outside this range is invalid (e.g. a negative or absurd movement).
AMOUNT_MIN = 0.0
AMOUNT_MAX = 50000.0


def _valid_event():
    """Build a well-formed event with a category and a numeric amount."""
    return {
        "event_id": str(uuid.uuid4()),
        "client_id": random.randint(1, 1_000_000),
        "category": random.choice(CATEGORIES),
        "amount": round(random.uniform(10.0, AMOUNT_MAX), 2),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def _invalid_event():
    """Build an invalid event, choosing one of three defects at random."""
    event = _valid_event()
    defect = random.choice(["missing_category", "out_of_range", "non_numeric"])
    if defect == "missing_category":
        del event["category"]
    elif defect == "out_of_range":
        event["amount"] = round(random.uniform(AMOUNT_MAX + 1, AMOUNT_MAX * 3), 2)
    else:  # non_numeric
        event["amount"] = "N/A"
    return event


def run(stream, minutes, rate, dup_rate, invalid_rate, region):
    client = boto3.client("kinesis", region_name=region)

    published = 0
    duplicates = 0
    invalid = 0
    recent_ids = []  # pool of past event_ids to repeat on purpose

    deadline = time.time() + minutes * 60
    interval = 1.0 / rate  # seconds between records, to hold a steady rate

    print(
        f"Publishing to {stream} for {minutes} min at ~{rate} ev/s "
        f"(dup={dup_rate}, invalid={invalid_rate})"
    )

    while time.time() < deadline:
        roll = random.random()

        if roll < invalid_rate:
            event = _invalid_event()
            invalid += 1
        elif roll < invalid_rate + dup_rate and recent_ids:
            # Repeat a whole past event, same event_id: a true duplicate.
            event = dict(random.choice(recent_ids))
            event["ts"] = datetime.now(timezone.utc).isoformat()
            duplicates += 1
        else:
            event = _valid_event()
            recent_ids.append(event)
            if len(recent_ids) > 200:
                recent_ids.pop(0)

        # Partition by category so a category stays on one shard and ordering
        # within a category is preserved; any value works for a single shard.
        partition_key = str(event.get("category", "invalid"))
        client.put_record(
            StreamName=stream,
            Data=(json.dumps(event) + "\n").encode("utf-8"),
            PartitionKey=partition_key,
        )
        published += 1

        if published % 100 == 0:
            print(f"  published={published} duplicates={duplicates} invalid={invalid}")

        time.sleep(interval)

    summary = {
        "published": published,
        "duplicates_sent": duplicates,
        "invalid_sent": invalid,
        "unique_valid_sent": published - duplicates - invalid,
    }
    print("\nSUMMARY (what the producer sent):")
    print(json.dumps(summary, indent=2))
    with open("producer_summary.json", "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
    print("Saved producer_summary.json")
    return summary


def main():
    parser = argparse.ArgumentParser(description="FINSIR Kinesis event producer")
    parser.add_argument("--stream", required=True, help="Kinesis stream name")
    parser.add_argument("--minutes", type=float, default=5.0, help="Run time (>= 5)")
    parser.add_argument("--rate", type=float, default=20.0, help="Events per second")
    parser.add_argument("--dup-rate", type=float, default=0.10, help="Fraction of duplicates")
    parser.add_argument("--invalid-rate", type=float, default=0.05, help="Fraction of invalids")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()

    if args.minutes < 5:
        print("WARNING: the enunciado asks for at least 5 minutes of publishing.")

    run(
        stream=args.stream,
        minutes=args.minutes,
        rate=args.rate,
        dup_rate=args.dup_rate,
        invalid_rate=args.invalid_rate,
        region=args.region,
    )


if __name__ == "__main__":
    main()
