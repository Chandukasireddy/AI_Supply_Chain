"""
Inventory Agent — two interfaces in one module:

  Standalone (new):
    get_inventory_risk()          -> list[dict]  items with stock_level < safety_stock * 1.2
    ask_inventory_agent(question) -> str         Gemini 2.5 Flash analysis over at-risk data

  LangGraph node (existing, used by workflow.py):
    make_inventory_node(client)   -> node fn     wraps legacy DuckDB/Spark DataClient
"""
from __future__ import annotations

import os
import subprocess
from dotenv import load_dotenv
load_dotenv()

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from google import genai
from google.genai import types as genai_types

_DATABRICKS_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
_AZ_PATHS = [
    "az",
    r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
]

CATALOG             = "db_supply_chain_workspace"
SCHEMA              = "supply_chain_data"
TABLE               = "inventory"
FQN                 = f"{CATALOG}.{SCHEMA}.{TABLE}"
LOW_STOCK_THRESHOLD = 1.2   # flag when stock_level < safety_stock * this


# ── Databricks SQL connector helpers ──────────────────────────────────────────

def _get_aad_token() -> str | None:
    """Get Azure AD token for Databricks via Azure CLI. Returns None if unavailable."""
    for az in _AZ_PATHS:
        try:
            result = subprocess.run(
                [az, "account", "get-access-token",
                 "--resource", _DATABRICKS_RESOURCE,
                 "--query", "accessToken", "-o", "tsv"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
    return None


def _get_connection(token: str):
    host         = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    return sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=token,
        catalog=CATALOG,
        schema=SCHEMA,
    )


_LOW_STOCK_QUERY = f"""
    SELECT
        i.product_id,
        i.product_name,
        i.category,
        i.stock_level,
        i.safety_stock,
        i.reorder_point,
        i.warehouse_location,
        i.region,
        i.unit_cost,
        i.lead_time_days,
        s.supplier_name,
        s.country          AS supplier_country,
        s.risk_tier        AS supplier_risk_tier,
        ROUND(i.safety_stock * {LOW_STOCK_THRESHOLD}, 1)        AS threshold,
        ROUND(i.stock_level * 1.0 / i.safety_stock, 3)         AS stock_ratio
    FROM {CATALOG}.{SCHEMA}.inventory  i
    LEFT JOIN {CATALOG}.{SCHEMA}.suppliers s ON i.supplier_id = s.supplier_id
    WHERE i.stock_level < i.safety_stock * {LOW_STOCK_THRESHOLD}
    ORDER BY stock_ratio ASC
"""


def _rows_via_sql_connector(token: str) -> list[dict]:
    with _get_connection(token) as conn:
        with conn.cursor() as cursor:
            cursor.execute(_LOW_STOCK_QUERY)
            columns = [d[0] for d in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _rows_via_sdk(token: str) -> list[dict]:
    import time
    host         = os.environ["DATABRICKS_HOST"]
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

    w    = WorkspaceClient(host=host, token=token)
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=_LOW_STOCK_QUERY,
        wait_timeout="50s",
    )

    state = resp.status.state
    if state in (StatementState.PENDING, StatementState.RUNNING):
        for _ in range(24):
            time.sleep(5)
            resp  = w.statement_execution.get_statement(resp.statement_id)
            state = resp.status.state
            if state not in (StatementState.PENDING, StatementState.RUNNING):
                break

    if state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise RuntimeError(f"SDK query failed [{state}]: {err.message if err else 'unknown'}")

    columns = [col.name for col in resp.manifest.schema.columns]
    return [
        dict(zip(columns, [c.str_value for c in row.values]))
        for row in (resp.result.data_array or [])
    ]


def _rows_via_cache() -> list[dict]:
    """Last-resort fallback: filter temp_inventory_data.json in Python."""
    import json as _json
    cache = "temp_inventory_data.json"
    if not os.path.exists(cache):
        raise FileNotFoundError(
            f"No cache file at {cache} and Databricks is unreachable. "
            "Seed the data first: python src/databricks/seed_data.py"
        )
    with open(cache) as f:
        all_rows = _json.load(f)

    rows = []
    for r in all_rows:
        sl, ss = int(r["stock_level"]), int(r["safety_stock"])
        threshold  = round(ss * LOW_STOCK_THRESHOLD, 1)
        stock_ratio = round(sl / ss, 3)
        if sl < threshold:
            rows.append({**r,
                         "stock_level": sl, "safety_stock": ss,
                         "threshold": threshold, "stock_ratio": stock_ratio})

    rows.sort(key=lambda r: r["stock_ratio"])
    return rows


def _is_scope_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "scope" in msg and "sql" in msg


def get_inventory_risk() -> list[dict]:
    """
    Returns items where stock_level < safety_stock * 1.2 (low-stock warning).

    Connection priority:
      1. sql-connector with PAT token
      2. sql-connector with Azure AD token (az login)
      3. SDK statement execution with Azure AD token
      4. temp_inventory_data.json cache (offline fallback)
    """
    print("[INVENTORY] Connecting to Databricks SQL ...", flush=True)
    pat   = os.environ.get("DATABRICKS_TOKEN", "")
    rows  = None

    # 1. PAT via sql-connector
    try:
        rows = _rows_via_sql_connector(pat)
        print("[INVENTORY] Connected via sql-connector (PAT).", flush=True)
    except Exception as exc:
        if not _is_scope_error(exc):
            raise
        print("[INVENTORY] PAT missing SQL scope -> trying Azure AD ...", flush=True)

    # 2 & 3. Azure AD token
    if rows is None:
        aad = _get_aad_token()
        if aad:
            try:
                rows = _rows_via_sql_connector(aad)
                print("[INVENTORY] Connected via sql-connector (Azure AD).", flush=True)
            except Exception:
                try:
                    rows = _rows_via_sdk(aad)
                    print("[INVENTORY] Connected via SDK (Azure AD).", flush=True)
                except Exception as exc2:
                    print(f"[INVENTORY] Azure AD SDK failed: {exc2}", flush=True)
        else:
            print("[INVENTORY] Azure AD unavailable (run: az login).", flush=True)

    # 4. Local cache
    if rows is None:
        print("[INVENTORY] Falling back to local cache.", flush=True)
        rows = _rows_via_cache()
        print("[INVENTORY] Loaded from cache.", flush=True)

    # SDK returns strings; normalise numeric types
    for r in rows:
        for col in ("stock_level", "safety_stock", "reorder_point", "lead_time_days"):
            if isinstance(r.get(col), str):
                r[col] = int(r[col])
        for col in ("threshold", "stock_ratio", "unit_cost"):
            if isinstance(r.get(col), str):
                r[col] = float(r[col])

    print(f"[INVENTORY] {len(rows)} at-risk item(s) found.", flush=True)
    return rows


# ── Gemini analysis layer ──────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are a supply-chain risk analyst specialising in semiconductor and electronics inventory.

IMPORTANT: Every row in the table below has ALREADY been flagged as at-risk because its
stock_level is below safety_stock * 1.2 (the early-warning threshold).

Risk interpretation by stock_ratio (= stock_level / safety_stock):
  stock_ratio < 1.0        CRITICAL  — stock is already BELOW the safety buffer
  1.0 <= stock_ratio < 1.2 WARNING   — dangerously close to depletion; action required
  stock_ratio >= 1.2       (healthy — excluded from this table)

Columns:
  product_name | category | stock | safety | reorder_pt | location | region |
  unit_cost_usd | lead_days | supplier | supplier_country | supplier_risk_tier | ratio

Warehouses: Stuttgart (Europe), Munich (Europe), Singapore (APAC),
            Austin TX (Americas), Shenzhen (APAC), Seoul (APAC),
            Taipei (APAC), Santa Clara (Americas)

Answer the user's question concisely. Cite product names, stock figures, risk level,
supplier name, and geographic region where relevant.
If no rows are present, say no items are currently flagged.
"""


def ask_inventory_agent(question: str) -> str:
    """
    Use Gemini 2.5 Flash to answer a natural-language question about
    at-risk inventory pulled live from Databricks.

    Example:
        ask_inventory_agent("Which items are at critical risk in Germany?")
    """
    print(f"\n[AGENT] Question: {question}", flush=True)

    risk_items = get_inventory_risk()

    if not risk_items:
        table_text = "No items are currently below the low-stock threshold."
    else:
        header = (
            f"{'product_name':<42} {'cat':<16} {'stk':>5} {'sfy':>5} "
            f"{'loc':<12} {'region':<9} {'cost':>8} {'ld':>4} "
            f"{'supplier':<24} {'s_ctry':<14} {'s_risk':<8} {'ratio':>6}"
        )
        divider = "-" * len(header)
        rows_txt = "\n".join(
            f"{r['product_name']:<42} {r.get('category',''):<16} "
            f"{r['stock_level']:>5} {r['safety_stock']:>5} "
            f"{r['warehouse_location']:<12} {r.get('region',''):<9} "
            f"{r.get('unit_cost',0):>8,.0f} {r.get('lead_time_days',0):>4} "
            f"{r.get('supplier_name',''):<24} {r.get('supplier_country',''):<14} "
            f"{r.get('supplier_risk_tier',''):<8} {r['stock_ratio']:>6.3f}"
            for r in risk_items
        )
        table_text = f"{header}\n{divider}\n{rows_txt}"

    print("[AGENT] Sending data to Gemini 2.5 Flash …", flush=True)
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=f"{_SYSTEM_PROMPT}\n\nAt-risk inventory:\n{table_text}\n\nQuestion: {question}",
        config=genai_types.GenerateContentConfig(temperature=0),
    )

    answer = response.text.strip()
    print("[AGENT] Gemini response received.\n", flush=True)
    return answer


# ── Data update helpers ───────────────────────────────────────────────────────

def update_stock(product_id: str, stock_level: int, safety_stock: int | None = None) -> None:
    """
    Update stock_level (and optionally safety_stock) for a product in Databricks.

    Example:
        update_stock("P001", stock_level=10, safety_stock=12)  # force into AT-RISK
        update_stock("P001", stock_level=45)                   # restore to healthy
    """
    pat = os.environ.get("DATABRICKS_TOKEN", "")
    token = None

    # Resolve auth token (same priority as get_inventory_risk)
    try:
        _rows_via_sql_connector(pat)  # quick scope check
        token = pat
    except Exception as exc:
        if _is_scope_error(exc):
            token = _get_aad_token()
        else:
            raise

    if not token:
        raise RuntimeError("No valid auth token — run: az login")

    set_clause = f"stock_level = {int(stock_level)}"
    if safety_stock is not None:
        set_clause += f", safety_stock = {int(safety_stock)}"

    dml = f"UPDATE {FQN} SET {set_clause} WHERE product_id = '{product_id}'"

    host         = os.environ["DATABRICKS_HOST"]
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    w = WorkspaceClient(host=host, token=token)
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=dml,
        wait_timeout="30s",
    )

    import time
    state = resp.status.state
    if state in (StatementState.PENDING, StatementState.RUNNING):
        for _ in range(12):
            time.sleep(3)
            resp  = w.statement_execution.get_statement(resp.statement_id)
            state = resp.status.state
            if state not in (StatementState.PENDING, StatementState.RUNNING):
                break

    if state != StatementState.SUCCEEDED:
        err = resp.status.error
        raise RuntimeError(f"UPDATE failed [{state}]: {err.message if err else 'unknown'}")

    print(f"[INVENTORY] Updated {product_id}: stock_level={stock_level}"
          + (f", safety_stock={safety_stock}" if safety_stock is not None else ""),
          flush=True)


# ── LangGraph node (legacy DataClient interface, used by workflow.py) ──────────

def make_inventory_node(client):
    """Returns a LangGraph-compatible node function backed by a DataClient."""
    from langchain_anthropic import ChatAnthropic
    from langchain_core.messages import HumanMessage
    from langchain_core.prompts import ChatPromptTemplate
    from src.databricks.queries import get_inventory_summary, get_low_stock_items, get_pending_orders
    from src.graph.state import AgentState

    _PROMPT = ChatPromptTemplate.from_messages([
        ("system",
         "You are an inventory analyst. Produce a concise, actionable summary. "
         "Focus on items needing attention (low stock, overdue orders)."),
        ("human",
         "Task: {task}\n\n"
         "--- Low-stock items ---\n{low_stock}\n\n"
         "--- Pending orders ---\n{pending}\n\n"
         "--- Inventory summary by category ---\n{summary}\n\n"
         "Summarise key findings in 3-5 bullet points."),
    ])

    llm   = ChatAnthropic(model="claude-sonnet-4-6", temperature=0, max_tokens=512)
    chain = _PROMPT | llm

    def inventory_node(state: AgentState) -> dict:
        low_stock = get_low_stock_items(client).to_string(index=False)
        pending   = get_pending_orders(client).to_string(index=False)
        summary   = get_inventory_summary(client).to_string(index=False)

        response = chain.invoke({
            "task":      state["task"],
            "low_stock": low_stock or "none",
            "pending":   pending   or "none",
            "summary":   summary   or "none",
        })

        return {
            "messages": [HumanMessage(content=response.content, name="inventory_agent")],
            "results":  {"inventory": response.content},
        }

    return inventory_node


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    question = " ".join(sys.argv[1:]) or "Which items are at critical risk in Germany?"
    print("=" * 60, flush=True)
    answer = ask_inventory_agent(question)
    print("=" * 60)
    print(answer)
    print("=" * 60)
