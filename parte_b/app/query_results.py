"""Query the Parte B DynamoDB tables and check that the counts reconcile.

Shows, for the video:
  * the per-window per-category results (count and average amount),
  * the answer to a project question (which category moves the most money),
  * the invalid events with their reason,
  * the reconciliation: published == unique + duplicates + invalid.

Usage:
    python query_results.py --prefix finsir --region us-east-1
"""

import argparse
import json
from collections import defaultdict

import boto3


def _scan_all(table):
    items = []
    kwargs = {}
    while True:
        page = table.scan(**kwargs)
        items.extend(page["Items"])
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def main():
    parser = argparse.ArgumentParser(description="Query Parte B results")
    parser.add_argument("--prefix", default="finsir", help="Resource name prefix (project)")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb", region_name=args.region)
    results = dynamodb.Table(f"{args.prefix}-ventanas")
    rejects = dynamodb.Table(f"{args.prefix}-invalidos")
    counters = dynamodb.Table(f"{args.prefix}-contadores")

    # 1. Per-window per-category results.
    rows = _scan_all(results)
    rows.sort(key=lambda r: (r["window_start"], r["category"]))
    print("=== Resultados por ventana y categoria ===")
    print(f"{'window_start':25} {'category':18} {'count':>8} {'avg_amount':>12}")
    for row in rows:
        print(
            f"{row['window_start']:25} {row['category']:18} "
            f"{int(row['event_count']):>8} {float(row['avg_amount']):>12.2f}"
        )

    # 2. Project question: which product category moves the most money overall?
    total_by_cat = defaultdict(float)
    count_by_cat = defaultdict(int)
    for row in rows:
        total_by_cat[row["category"]] += float(row.get("sum_amount", 0))
        count_by_cat[row["category"]] += int(row["event_count"])
    print("\n=== Pregunta del proyecto: categoria con mayor monto total movido ===")
    ranking = sorted(total_by_cat.items(), key=lambda kv: kv[1], reverse=True)
    for category, total in ranking:
        avg = total / count_by_cat[category] if count_by_cat[category] else 0
        print(f"  {category:18} total={total:14.2f}  eventos={count_by_cat[category]:>7}  avg={avg:.2f}")
    if ranking:
        print(f"  -> Respuesta: la categoria con mayor monto es '{ranking[0][0]}'.")

    # 3. Invalid events with their reason.
    bad = _scan_all(rejects)
    reason_counts = defaultdict(int)
    for item in bad:
        reason_counts[item["reason"]] += 1
    print("\n=== Eventos invalidos apartados, por motivo ===")
    for reason, n in sorted(reason_counts.items(), key=lambda kv: kv[1], reverse=True):
        print(f"  {reason:22} {n}")

    # 4. Reconciliation of the counts.
    counter_items = {c["counter"]: int(c["value"]) for c in _scan_all(counters)}
    unique = counter_items.get("unique_processed", 0)
    duplicates = counter_items.get("duplicates_discarded", 0)
    invalid = counter_items.get("invalid_discarded", 0)
    processed_total = unique + duplicates + invalid
    print("\n=== Conteos (lado del consumidor de ventanas) ===")
    print(f"  unique_processed      = {unique}")
    print(f"  duplicates_discarded  = {duplicates}")
    print(f"  invalid_discarded     = {invalid}")
    print(f"  suma                  = {processed_total}")
    print(
        "\n  Comparalo con 'published' de producer_summary.json: deben coincidir\n"
        "  published == unique_processed + duplicates_discarded + invalid_discarded"
    )

    try:
        with open("producer_summary.json", encoding="utf-8") as handle:
            published = json.load(handle)["published"]
        status = "CUADRAN" if published == processed_total else "NO CUADRAN"
        print(f"\n  published (producer)  = {published}")
        print(f"  Resultado: {status}")
    except FileNotFoundError:
        print("\n  (producer_summary.json no encontrado; comparalo a mano)")


if __name__ == "__main__":
    main()
