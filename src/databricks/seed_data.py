"""
Generates 15 rows of synthetic supply-chain inventory data with Gemini 2.0 Flash,
then creates and populates db_supply_chain_workspace.supply_chain_data.inventory
via the Databricks SDK statement-execution API.

Usage:
    python src/databricks/seed_data.py
"""
from __future__ import annotations

# ── load_dotenv FIRST so every os.environ.get() below sees the .env values ──
from dotenv import load_dotenv
load_dotenv()

import json
import os
import sys
import time
import traceback

from google import genai
from google.genai import types as genai_types
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.sql import StatementState

CATALOG    = "db_supply_chain_workspace"
CACHE_FILE = "temp_inventory_data.json"
SCHEMA  = "supply_chain_data"
TABLE   = "inventory"
FQN     = f"{CATALOG}.{SCHEMA}.{TABLE}"

_GEMINI_PROMPT = """
Generate exactly 15 rows of realistic supply-chain inventory data for high-value electronics.

Product focus (mix these and similar items):
- Nvidia H100 SXM5 80GB GPU
- Nvidia A100 PCIe 40GB GPU
- Bosch LiDAR Sensor LRR4
- Bosch Radar Sensor SRR3
- TSMC 3nm Wafer Batch
- Samsung HBM3 Memory Stack
- ASML EUV Lens Module
- Qualcomm Snapdragon X Elite SoC
- Intel Gaudi 3 AI Accelerator
- Mobileye EyeQ6H Vision Chip
- Infineon IGBT Power Module
- STMicro SiC MOSFET 1200V
- Texas Instruments TDA4VM SoC
- Renesas RH850 Automotive MCU
- AMD Instinct MI300X GPU

Warehouse locations to use (rotate across rows):
Stuttgart, Singapore, Austin TX, Shenzhen, Seoul, Taipei, Munich, Santa Clara

Return ONLY a valid JSON array with exactly 15 objects. Each object must have:
  "product_id"         : string  — sequential like "P001", "P002", ..., "P015"
  "product_name"       : string  — specific product with model/variant
  "stock_level"        : integer — realistic current units in stock (5-500)
  "safety_stock"       : integer — minimum buffer (always < stock_level)
  "warehouse_location" : string  — one of the cities listed above

No markdown, no explanation — raw JSON array only.
"""


def _check_env() -> str:
    """Validate required env vars and return warehouse_id."""
    print("[ENV] Checking environment variables …", flush=True)

    missing = []
    for var in ("GEMINI_API_KEY", "DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID"):
        val = os.environ.get(var)
        masked = f"{val[:6]}…" if val and len(val) > 6 else ("(not set)" if not val else val)
        print(f"  {var:35s} = {masked}", flush=True)
        if not val:
            missing.append(var)

    if missing:
        raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")

    print("[ENV] All required variables present.\n", flush=True)
    return os.environ["DATABRICKS_WAREHOUSE_ID"]


def _load_or_generate_rows() -> list[dict]:
    if os.path.exists(CACHE_FILE):
        print(f"[CACHE] Found {CACHE_FILE} — loading cached data (skipping Gemini call).", flush=True)
        with open(CACHE_FILE) as f:
            rows = json.load(f)
        print(f"[CACHE] Loaded {len(rows)} rows from cache.\n", flush=True)
        return rows

    print(f"[CACHE] No cache file found. Calling Gemini …", flush=True)
    rows = _generate_rows()

    with open(CACHE_FILE, "w") as f:
        json.dump(rows, f, indent=2)
    print(f"[CACHE] Saved to {CACHE_FILE} (delete to regenerate).\n", flush=True)
    return rows


def _generate_rows() -> list[dict]:
    print("[GEMINI] Starting Gemini 2.5 Flash …", flush=True)

    api_key = os.environ["GEMINI_API_KEY"]
    client = genai.Client(api_key=api_key)

    print("[GEMINI] Sending prompt …", flush=True)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=_GEMINI_PROMPT,
        config=genai_types.GenerateContentConfig(
            response_mime_type="application/json",
        ),
    )

    print("[GEMINI] Response received. Parsing JSON …", flush=True)
    rows: list[dict] = json.loads(response.text.strip())
    print(f"[GEMINI] Gemini generated {len(rows)} rows.", flush=True)

    if len(rows) != 15:
        raise ValueError(f"Expected 15 rows, got {len(rows)}.")

    required = {"product_id", "product_name", "stock_level", "safety_stock", "warehouse_location"}
    for i, row in enumerate(rows):
        missing = required - row.keys()
        if missing:
            raise ValueError(f"Row {i} missing fields: {missing}")

    print("[GEMINI] All rows validated.", flush=True)
    return rows


def _run_sql(w: WorkspaceClient, warehouse_id: str, sql: str, label: str) -> None:
    print(f"[SQL] Executing: {label} …", flush=True)

    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )

    state = resp.status.state
    print(f"[SQL] Initial state: {state}", flush=True)

    if state in (StatementState.PENDING, StatementState.RUNNING):
        stmt_id = resp.statement_id
        print(f"[SQL] Polling statement {stmt_id} …", flush=True)
        for attempt in range(24):
            time.sleep(5)
            resp = w.statement_execution.get_statement(stmt_id)
            state = resp.status.state
            print(f"[SQL]   poll {attempt + 1:02d}: {state}", flush=True)
            if state not in (StatementState.PENDING, StatementState.RUNNING):
                break

    if state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise RuntimeError(
            f"[SQL] {label} failed [{state}]: {err.message if err else 'unknown error'}"
        )

    print(f"[SQL] {label} — SUCCEEDED.\n", flush=True)


def _escape(val: str) -> str:
    return val.replace("'", "''")


def _write_sql_fallback(rows: list[dict], path: str = "seed_inventory.sql") -> None:
    lines = [
        f"-- Auto-generated fallback: {FQN}",
        f"-- {len(rows)} rows  |  run in Databricks SQL Editor or CLI",
        "",
        f"CREATE TABLE IF NOT EXISTS {FQN} (",
        "    product_id          STRING  NOT NULL,",
        "    product_name        STRING  NOT NULL,",
        "    stock_level         INT     NOT NULL,",
        "    safety_stock        INT     NOT NULL,",
        "    warehouse_location  STRING  NOT NULL",
        ") USING DELTA;",
        "",
    ]
    for r in rows:
        lines.append(
            f"INSERT INTO {FQN} VALUES "
            f"('{_escape(r['product_id'])}', '{_escape(r['product_name'])}', "
            f"{int(r['stock_level'])}, {int(r['safety_stock'])}, "
            f"'{_escape(r['warehouse_location'])}');"
        )

    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"[FALLBACK] Written {len(rows)} INSERT statements to {path}", flush=True)
    print(f"[FALLBACK] Run it manually in Databricks SQL Editor or with the CLI.", flush=True)


def main() -> None:
    print("=" * 60, flush=True)
    print("  Databricks Inventory Seed Script", flush=True)
    print("=" * 60 + "\n", flush=True)

    # ── Step 0: env check ────────────────────────────────────────────────────
    warehouse_id = _check_env()

    # ── Step 1: generate data (cached) ──────────────────────────────────────
    print("STEP 1/3  Generate synthetic data", flush=True)
    print("-" * 40, flush=True)
    rows = _load_or_generate_rows()

    print("Generated rows preview:", flush=True)
    for r in rows:
        print(
            f"  {r['product_id']}  {r['product_name']:<42}"
            f"  stock={r['stock_level']:>4}  safety={r['safety_stock']:>4}"
            f"  {r['warehouse_location']}",
            flush=True,
        )

    # ── Step 2: connect & create table ──────────────────────────────────────
    print(f"\nSTEP 2/3  Create table {FQN}", flush=True)
    print("-" * 40, flush=True)
    print("[DATABRICKS] Connecting to workspace …", flush=True)
    host  = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    w = WorkspaceClient(host=host, token=token)
    print(f"[DATABRICKS] Connected to {host}. Warehouse ID: {warehouse_id}\n", flush=True)

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {FQN} (
        product_id          STRING  NOT NULL,
        product_name        STRING  NOT NULL,
        stock_level         INT     NOT NULL,
        safety_stock        INT     NOT NULL,
        warehouse_location  STRING  NOT NULL
    )
    USING DELTA
    """

    values_clause = ",\n    ".join(
        f"('{_escape(r['product_id'])}', '{_escape(r['product_name'])}', "
        f"{int(r['stock_level'])}, {int(r['safety_stock'])}, "
        f"'{_escape(r['warehouse_location'])}')"
        for r in rows
    )
    insert_sql = f"INSERT INTO {FQN} VALUES\n    {values_clause}"

    try:
        _run_sql(w, warehouse_id, ddl, f"CREATE TABLE {FQN}")
        print("[DATABRICKS] Table created (or already exists).", flush=True)

        # ── Step 3: insert rows ──────────────────────────────────────────────
        print(f"\nSTEP 3/3  Insert {len(rows)} rows", flush=True)
        print("-" * 40, flush=True)
        _run_sql(w, warehouse_id, insert_sql, f"INSERT {len(rows)} rows into {FQN}")

    except PermissionDenied as exc:
        print(f"\n[WARN] SQL Scope Error detected: {exc}", flush=True)
        print("SQL Scope Error detected. Generating SQL file for manual import …", flush=True)
        _write_sql_fallback(rows)
        return

    print("=" * 60, flush=True)
    print(f"  Done. {FQN}", flush=True)
    print(f"  Populated with {len(rows)} rows.", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\n[ERROR] Script failed: {exc}", flush=True)
        print("\n--- Full traceback ---", flush=True)
        traceback.print_exc()
        sys.exit(1)
