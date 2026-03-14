import argparse
import json
import os
import sys
import time
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import requests
from postgrest.exceptions import APIError
from supabase import Client, create_client


def _env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        if default is not None:
            return default
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _print_env_help(missing: str) -> None:
    msg = (
        "Missing required environment variable: "
        + missing
        + "\n\n"
        + "Set these env vars before running:\n"
        + "  SUPABASE_URL\n"
        + "  SUPABASE_SERVICE_ROLE_KEY\n"
        + "  BACKEND_BASE_URL\n"
        + "Optional:\n"
        + "  BACKEND_AUTH_HEADER   (example: 'Bearer <token>')\n"
        + "  TRACKER_ENGINE_MODE   ('all' or 'suggested')\n"
        + "  TRACKER_INTERVAL_SECONDS (default 86400)\n"
        + "  TRACKER_MAX_PRODUCTS  (0 = no limit)\n"
        + "  TRACKER_TIMEOUT_SECONDS (default 60)\n"
    )
    print(msg, file=sys.stderr)


def _optional_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    return value


def get_supabase() -> Client:
    url = _env("SUPABASE_URL")
    key = _env("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def _post_json(url: str, payload: Dict[str, Any], auth_header: Optional[str], timeout_s: int) -> requests.Response:
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if auth_header:
        headers["Authorization"] = auth_header
    return requests.post(url, data=json.dumps(payload), headers=headers, timeout=timeout_s)


def already_triggered_today_for_batch(supabase: Client, product_id: str, batch_id: str, run_date: date) -> bool:
    resp = (
        supabase.table("tracker_runs")
        .select("id")
        .eq("product_id", product_id)
        .eq("batch_id", batch_id)
        .eq("run_date", run_date.isoformat())
        .limit(1)
        .execute()
    )
    data = getattr(resp, "data", None) or []
    return len(data) > 0


def record_tracker_run(
    supabase: Client,
    product_id: str,
    user_id: str,
    batch_id: Optional[str],
    snapshot_id: Optional[str],
    run_date: date,
    status: str,
    error: Optional[str],
) -> None:
    row: Dict[str, Any] = {
        "product_id": product_id,
        "user_id": user_id,
        "batch_id": batch_id,
        "snapshot_id": snapshot_id,
        "run_date": run_date.isoformat(),
        "status": status,
        "error": error,
    }
    try:
        supabase.table("tracker_runs").insert(row).execute()
    except Exception as e:
        code: Optional[str] = None
        try:
            if isinstance(e, APIError) and getattr(e, "args", None) and isinstance(e.args[0], dict):
                code = e.args[0].get("code")
            elif getattr(e, "args", None) and isinstance(e.args[0], dict):
                code = e.args[0].get("code")
        except Exception:
            code = None

        if code == "23505" or "'code': '23505'" in str(e) or "code': '23505'" in str(e) or "23505" in str(e):
            return
        raise


def fetch_latest_snapshot_id_for_batch(supabase: Client, batch_id: str, product_id: str) -> Optional[str]:
    resp = (
        supabase.table("analysis_snapshots")
        .select("id")
        .eq("batch_id", batch_id)
        .eq("product_id", product_id)
        .order("started_at", desc=True)
        .limit(1)
        .execute()
    )
    data = getattr(resp, "data", None) or []
    if not data:
        return None
    return data[0]["id"]


def fetch_daily_products(supabase: Client, limit: int) -> List[Dict[str, Any]]:
    q = supabase.table("products").select("*").eq("daily_tracker", True)
    if limit > 0:
        q = q.limit(limit)
    resp = q.execute()
    return list(getattr(resp, "data", None) or [])


def fetch_latest_daily_batch_id(supabase: Client, product_id: str) -> Optional[str]:
    resp = (
        supabase.table("query_batches")
        .select("id")
        .eq("product_id", product_id)
        .eq("daily_tracker", True)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    data = getattr(resp, "data", None) or []
    if not data:
        return None
    return data[0]["id"]


def fetch_engine_queries_for_snapshot(
    supabase: Client,
    snapshot_id: str,
    mode: str,
) -> Tuple[List[str], List[str], List[str]]:
    chatgpt_enabled_raw = (os.getenv("CHATGPT_ANALYSIS") or "").strip().lower()
    chatgpt_enabled = chatgpt_enabled_raw in ("1", "true", "yes", "y", "on")

    google_resp = (
        supabase.table("product_analysis_google")
        .select("search_query")
        .eq("snapshot_id", snapshot_id)
        .order("created_at", desc=False)
        .execute()
    )
    google_rows = getattr(google_resp, "data", None) or []
    google = [r["search_query"] for r in google_rows if r.get("search_query")]

    perplexity_resp = (
        supabase.table("product_analysis_perplexity")
        .select("optimization_prompt")
        .eq("snapshot_id", snapshot_id)
        .order("created_at", desc=False)
        .execute()
    )
    perplexity_rows = getattr(perplexity_resp, "data", None) or []
    perplexity = [r["optimization_prompt"] for r in perplexity_rows if r.get("optimization_prompt")]

    chatgpt: List[str] = []
    if chatgpt_enabled:
        chatgpt_resp = (
            supabase.table("product_analysis_chatgpt")
            .select("optimization_prompt")
            .eq("snapshot_id", snapshot_id)
            .order("created_at", desc=False)
            .execute()
        )
        chatgpt_rows = getattr(chatgpt_resp, "data", None) or []
        chatgpt = [r["optimization_prompt"] for r in chatgpt_rows if r.get("optimization_prompt")]

    if mode == "all":
        return google, perplexity, chatgpt

    return google, perplexity, chatgpt


def product_to_client_json(product: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": product.get("id"),
        "product_name": product.get("product_name"),
        "product_url": product.get("product_url"),
        "description": product.get("description"),
        "specifications": product.get("specifications"),
        "features": product.get("features"),
        "targeted_market": product.get("targeted_market"),
        "problem_product_is_solving": product.get("problem_product_is_solving"),
        "general_product_type": product.get("general_product_type"),
        "specific_product_type": product.get("specific_product_type"),
    }


def trigger_reanalysis(
    *,
    backend_base_url: str,
    auth_header: Optional[str],
    timeout_s: int,
    product: Dict[str, Any],
    batch_id: str,
    google_queries: List[str],
    perplexity_queries: List[str],
    chatgpt_queries: List[str],
) -> str:
    product_id = product["id"]
    user_id = product["user_id"]

    payload: Dict[str, Any] = {
        "product_id": product_id,
        "user_id": user_id,
        "batch_id": batch_id,
        "google_queries": google_queries,
        "perplexity_queries": perplexity_queries,
        "chatgpt_queries": chatgpt_queries,
        "client_product_json": product_to_client_json(product),
        "total_no_of_query": len(set(google_queries + perplexity_queries + chatgpt_queries)),
    }

    url = backend_base_url.rstrip("/") + "/api/v1/optimize/start"
    resp = _post_json(url, payload, auth_header, timeout_s)
    if resp.status_code >= 400:
        raise RuntimeError(f"Optimize start failed ({resp.status_code}): {resp.text}")

    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"Optimize start returned non-JSON response ({resp.status_code}): {resp.text}") from e

    snapshot_id = data.get("snapshot_id")
    if not snapshot_id:
        raise RuntimeError(f"Optimize start response missing snapshot_id ({resp.status_code}): {data}")
    return str(snapshot_id)


def run_once(
    *,
    supabase: Client,
    backend_base_url: str,
    auth_header: Optional[str],
    timeout_s: int,
    max_products: int,
    engine_mode: str,
) -> int:
    today = date.today()
    products = fetch_daily_products(supabase, max_products)
    failures = 0

    for product in products:
        product_id = product.get("id")
        user_id = product.get("user_id")
        if not product_id or not user_id:
            continue

        batch_id: Optional[str] = None
        snapshot_id: Optional[str] = None
        try:
            batch_id = fetch_latest_daily_batch_id(supabase, product_id)
            if not batch_id:
                record_tracker_run(
                    supabase,
                    product_id,
                    user_id,
                    None,
                    None,
                    today,
                    "skipped",
                    "No daily query batch found for product",
                )
                continue

            snapshot_id = fetch_latest_snapshot_id_for_batch(supabase, batch_id, product_id)
            if not snapshot_id:
                record_tracker_run(
                    supabase,
                    product_id,
                    user_id,
                    batch_id,
                    None,
                    today,
                    "skipped",
                    "No snapshot found for batch - cannot reanalyze",
                )
                continue

            if already_triggered_today_for_batch(supabase, product_id, batch_id, today):
                continue

            google_q, perplexity_q, chatgpt_q = fetch_engine_queries_for_snapshot(supabase, snapshot_id, engine_mode)

            if not (google_q or perplexity_q or chatgpt_q):
                record_tracker_run(
                    supabase,
                    product_id,
                    user_id,
                    batch_id,
                    snapshot_id,
                    today,
                    "skipped",
                    "No analyzed queries found for snapshot",
                )
                continue

            snapshot_id = trigger_reanalysis(
                backend_base_url=backend_base_url,
                auth_header=auth_header,
                timeout_s=timeout_s,
                product=product,
                batch_id=batch_id,
                google_queries=google_q,
                perplexity_queries=perplexity_q,
                chatgpt_queries=chatgpt_q,
            )

            record_tracker_run(
                supabase,
                product_id,
                user_id,
                batch_id,
                snapshot_id,
                today,
                "triggered",
                None,
            )
        except Exception as e:
            failures += 1
            msg = str(e)
            try:
                record_tracker_run(
                    supabase,
                    product_id,
                    user_id,
                    batch_id,
                    snapshot_id,
                    today,
                    "failed",
                    msg,
                )
            except Exception:
                pass
            print(f"Tracker failed for product_id={product_id}: {msg}", file=sys.stderr)

    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=int(os.getenv("TRACKER_INTERVAL_SECONDS", "86400")))
    parser.add_argument("--max-products", type=int, default=int(os.getenv("TRACKER_MAX_PRODUCTS", "0")))
    parser.add_argument(
        "--engine-mode",
        choices=["all", "suggested"],
        default=os.getenv("TRACKER_ENGINE_MODE", "all"),
    )
    parser.add_argument("--timeout-seconds", type=int, default=int(os.getenv("TRACKER_TIMEOUT_SECONDS", "60")))
    args = parser.parse_args()

    try:
        supabase = get_supabase()
        backend_base_url = _env("BACKEND_BASE_URL")
    except RuntimeError as e:
        prefix = "Missing required environment variable: "
        raw = str(e)
        missing = raw[len(prefix) :] if raw.startswith(prefix) else raw
        _print_env_help(missing)
        return 2

    auth_header = _optional_env("BACKEND_AUTH_HEADER")

    max_products = args.max_products
    if max_products < 0:
        max_products = 0

    if args.once:
        return run_once(
            supabase=supabase,
            backend_base_url=backend_base_url,
            auth_header=auth_header,
            timeout_s=args.timeout_seconds,
            max_products=max_products,
            engine_mode=args.engine_mode,
        )

    while True:
        run_once(
            supabase=supabase,
            backend_base_url=backend_base_url,
            auth_header=auth_header,
            timeout_s=args.timeout_seconds,
            max_products=max_products,
            engine_mode=args.engine_mode,
        )
        time.sleep(max(1, args.interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())