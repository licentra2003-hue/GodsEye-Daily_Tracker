import argparse
import json
import os
import sys
import uuid
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from supabase import Client, create_client

import main


def _load_env_file(env_path: str) -> None:
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            if "=" not in raw:
                continue
            k, v = raw.split("=", 1)
            k = k.strip()
            v = v.strip()
            if k and k not in os.environ:
                os.environ[k] = v


def get_supabase() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
    return create_client(url, key)


def build_payload_for_product(
    *,
    supabase: Client,
    product_id: str,
    engine_mode: str,
) -> Tuple[Dict[str, Any], str, str, str]:
    prod_resp = supabase.table("products").select("*").eq("id", product_id).limit(1).execute()
    products = getattr(prod_resp, "data", None) or []
    if not products:
        raise RuntimeError(f"Product not found: {product_id}")
    product = products[0]

    batch_id = main.fetch_latest_daily_batch_id(supabase, product_id)
    if not batch_id:
        print(f"No daily-tracked batch found for product {product_id}. Ensure query_batches.daily_tracker=true for at least one batch.", file=sys.stderr)
        sys.exit(1)

    snapshot_id = main.fetch_latest_snapshot_id_for_batch(supabase, batch_id)
    if not snapshot_id:
        print(f"No snapshot found for batch {batch_id} - cannot reanalyze. Ensure analysis_snapshots has a row for this batch.", file=sys.stderr)
        sys.exit(1)

    google_q, perplexity_q, chatgpt_q = main.fetch_engine_queries_for_snapshot(supabase, snapshot_id, engine_mode)

    payload: Dict[str, Any] = {
        "product_id": product_id,
        "user_id": product["user_id"],
        "batch_id": batch_id,
        "google_queries": google_q,
        "perplexity_queries": perplexity_q,
        "chatgpt_queries": chatgpt_q,
        "client_product_json": main.product_to_client_json(product),
        "total_no_of_query": len(set(google_q + perplexity_q + chatgpt_q)),
    }
    return payload, product["user_id"], batch_id, snapshot_id


def main_cli() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=os.path.join(os.path.dirname(__file__), ".env"))
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--engine-mode", choices=["all", "suggested"], default=os.getenv("TRACKER_ENGINE_MODE", "all"))
    parser.add_argument("--start-path", default="/api/v1/optimize/start")
    parser.add_argument("--print-only", action="store_true")
    parser.add_argument("--dummy-backend", action="store_true")
    args = parser.parse_args()

    _load_env_file(args.env)

    supabase = get_supabase()
    payload, user_id, batch_id, snapshot_id = build_payload_for_product(
        supabase=supabase,
        product_id=args.product_id,
        engine_mode=args.engine_mode,
    )

    print(json.dumps(payload, indent=2))

    if args.print_only:
        return 0

    if args.dummy_backend:
        fake_response = {"snapshot_id": snapshot_id, "status": "running", "total_queries": payload["total_no_of_query"]}
        print(json.dumps(fake_response, indent=2))

        main.record_tracker_run(
            supabase,
            payload["product_id"],
            user_id,
            batch_id,
            snapshot_id,
            date.today(),
            "triggered",
            None,
        )
        print("Inserted tracker_runs row for snapshot_id:", snapshot_id)
        return 0

    backend_base_url = os.getenv("BACKEND_BASE_URL")
    if not backend_base_url:
        raise RuntimeError("BACKEND_BASE_URL must be set")

    auth_header = os.getenv("BACKEND_AUTH_HEADER")

    url = backend_base_url.rstrip("/") + "/" + args.start_path.lstrip("/")
    resp = main._post_json(url, payload, auth_header, timeout_s=int(os.getenv("TRACKER_TIMEOUT_SECONDS", "60")))

    if resp.status_code != 202 and resp.status_code != 200:
        print(f"Non-accepted status: {resp.status_code} {resp.text}", file=sys.stderr)
        if resp.status_code == 404:
            print(
                "This usually means BACKEND_BASE_URL is wrong, or the server does not expose this path. "
                "Try adjusting BACKEND_BASE_URL or run with --start-path to match your deployed route.",
                file=sys.stderr,
            )
        return 3

    try:
        data = resp.json()
    except Exception:
        print(f"Accepted response was not JSON: {resp.text}", file=sys.stderr)
        return 4

    snapshot_id = data.get("snapshot_id")
    if not snapshot_id:
        print(f"Response missing snapshot_id: {data}", file=sys.stderr)
        return 5

    # Insert a tracking row only after confirmation
    main.record_tracker_run(
        supabase,
        payload["product_id"],
        user_id,
        batch_id,
        str(snapshot_id),
        date.today(),
        "triggered",
        None,
    )

    print("Inserted tracker_runs row for snapshot_id:", snapshot_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main_cli())
