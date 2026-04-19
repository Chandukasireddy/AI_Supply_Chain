"""
src/databricks/seed_v2.py
Push a realistic 5-table supply chain dataset to Databricks Delta Lake.

Tables in db_supply_chain_workspace.supply_chain_data:
  suppliers            38 rows   — structured + free-text risk_notes
  inventory           500 rows   — 100 products x 5 warehouses; ZORDER + CDF
  orders           20,000 rows   — 2-year history; PARTITIONED BY (year, month)
  shipments         8,000 rows   — PARTITIONED BY status
  disruption_events   300 rows   — VARIANT column (different JSON per event type)

Databricks features demonstrated:
  VARIANT           — one column, different JSON schema per row (no fixed schema)
  PARTITIONED BY    — query pruning on orders by year/month
  ZORDER BY         — co-locate data by product_id + warehouse_id on inventory
  Change Data Feed  — row-level change tracking on inventory (CDC)
  Time Travel       — SELECT * FROM orders VERSION AS OF N
  MERGE INTO        — atomic upsert (used by inventory_agent.update_stock)

Usage:
  python src/databricks/seed_v2.py
"""
from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import time
import traceback
from datetime import date, timedelta

import numpy as np
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# ── Config ────────────────────────────────────────────────────────────────────
CATALOG = "db_supply_chain_workspace"
SCHEMA  = "supply_chain_data"
FQN     = f"{CATALOG}.{SCHEMA}"

_DATABRICKS_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
_AZ_PATHS = ["az", r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"]

RNG  = np.random.default_rng(42)
RAND = random.Random(42)


# ── Reference data ────────────────────────────────────────────────────────────

WAREHOUSES = [
    ("WH01", "Stuttgart",    "Europe"),
    ("WH02", "Munich",       "Europe"),
    ("WH03", "Singapore",    "APAC"),
    ("WH04", "Austin TX",    "Americas"),
    ("WH05", "Shenzhen",     "APAC"),
    ("WH06", "Seoul",        "APAC"),
    ("WH07", "Taipei",       "APAC"),
    ("WH08", "Santa Clara",  "Americas"),
]

# (product_id, name, category, unit_cost_usd, lead_time_days)
PRODUCTS = [
    # AI Accelerators
    ("P001","Nvidia H100 SXM5 80GB GPU",           "AI_Accelerator", 28000, 45),
    ("P002","Nvidia H100 PCIe 80GB GPU",            "AI_Accelerator", 25000, 40),
    ("P003","Nvidia H200 SXM5 141GB GPU",           "AI_Accelerator", 38000, 60),
    ("P004","Nvidia A100 PCIe 40GB GPU",            "AI_Accelerator", 10000, 30),
    ("P005","Nvidia L40S 48GB GPU",                 "AI_Accelerator", 12000, 35),
    ("P006","AMD Instinct MI300X 192GB GPU",        "AI_Accelerator", 15000, 42),
    ("P007","AMD Instinct MI250X 128GB GPU",        "AI_Accelerator",  9000, 30),
    ("P008","Intel Gaudi 3 AI Accelerator",         "AI_Accelerator",  8500, 28),
    ("P009","Intel Gaudi 2 AI Accelerator",         "AI_Accelerator",  6500, 25),
    ("P010","Google TPU v5e 4-chip Module",         "AI_Accelerator", 18000, 90),
    ("P011","Cerebras CS-3 Wafer Scale Engine",     "AI_Accelerator",180000,120),
    ("P012","Groq LPU Inference Chip",              "AI_Accelerator",  4200, 35),
    ("P013","SambaNova SN40L RDU",                  "AI_Accelerator", 22000, 70),
    ("P014","Graphcore C600 IPU",                   "AI_Accelerator",  7500, 45),
    ("P015","Nvidia A10G 24GB GPU",                 "AI_Accelerator",  3500, 21),
    # Memory
    ("P016","Samsung HBM3 24GB Stack",              "Memory",          8000, 35),
    ("P017","SK Hynix HBM3E 36GB Stack",            "Memory",          9500, 40),
    ("P018","Micron HBM3 24GB Stack",               "Memory",          7800, 38),
    ("P019","Samsung DDR5 32GB RDIMM 6400MHz",      "Memory",           280, 14),
    ("P020","Micron DDR5 64GB RDIMM 5600MHz",       "Memory",           520, 14),
    ("P021","SK Hynix LPDDR5X 16GB Package",        "Memory",            95, 10),
    ("P022","Samsung LPDDR5 12GB Package",          "Memory",            78, 10),
    ("P023","Micron GDDR6X 16GB Module",            "Memory",           340, 21),
    ("P024","KIOXIA 3D NAND 512GB QLC",             "Memory",            62, 12),
    ("P025","Western Digital 3D NAND 1TB TLC",      "Memory",            85, 12),
    # Automotive Semiconductors
    ("P026","Bosch LiDAR Sensor LRR4",              "Automotive",       950, 30),
    ("P027","Bosch Radar Sensor SRR3",              "Automotive",       480, 28),
    ("P028","Mobileye EyeQ6H Vision Chip",          "Automotive",       380, 35),
    ("P029","Mobileye EyeQ5H Vision Chip",          "Automotive",       240, 28),
    ("P030","Texas Instruments TDA4VM SoC",         "Automotive",       320, 25),
    ("P031","Texas Instruments TDA4AL SoC",         "Automotive",       215, 22),
    ("P032","Renesas RH850/U2A MCU",                "Automotive",        42, 18),
    ("P033","NXP S32G3 Vehicle Network Processor",  "Automotive",       185, 24),
    ("P034","NXP i.MX 95 Application Processor",    "Automotive",       145, 21),
    ("P035","Infineon TC397 AURIX MCU",             "Automotive",        68, 20),
    ("P036","STMicro STA8910 Automotive SoC",       "Automotive",        95, 22),
    ("P037","Continental ARS540 Radar Module",      "Automotive",       720, 35),
    ("P038","Qualcomm Snapdragon Ride Elite SoC",   "Automotive",       275, 30),
    ("P039","Ambarella CV3 AI SoC",                 "Automotive",       185, 28),
    ("P040","Aptiv ADAS Fusion ECU",                "Automotive",      1200, 45),
    # Power Electronics
    ("P041","Infineon IGBT Module FF600R12ME4",     "Power",            280, 20),
    ("P042","STMicro SiC MOSFET 1200V 40A",         "Power",             18, 12),
    ("P043","Wolfspeed SiC MOSFET 1700V 72A",       "Power",             45, 16),
    ("P044","ON Semi SiC MOSFET Module 900V",       "Power",             38, 14),
    ("P045","Fuji Electric IGBT 2-in-1 Module",     "Power",            320, 24),
    ("P046","Mitsubishi IGBT Module CM400DY-34A",   "Power",            480, 28),
    ("P047","Semikron GaN Half-Bridge Module",      "Power",            155, 18),
    ("P048","Texas Instruments GaN FET 100V",       "Power",             12, 10),
    ("P049","EPC GaN Power Transistor eGaN 200V",   "Power",             28, 12),
    ("P050","Navitas GaN IC 650V Fast Charger",     "Power",              8, 10),
    # Networking
    ("P051","Broadcom Tomahawk 5 Switch ASIC",      "Networking",      4200, 42),
    ("P052","Broadcom Jericho3 AI Network Chip",    "Networking",      6800, 45),
    ("P053","Marvell Prestera 98DX9000 Switch",     "Networking",      3600, 38),
    ("P054","Intel Tofino 3 Programmable Switch",   "Networking",      5200, 42),
    ("P055","Cisco Silicon One G200 ASIC",          "Networking",      4800, 40),
    ("P056","Marvell Alaska Ultra 400G PHY",        "Networking",       890, 28),
    ("P057","Broadcom 400G Optical DSP Chip",       "Networking",      1200, 32),
    ("P058","Intel E810 100GbE NIC Controller",     "Networking",       320, 18),
    ("P059","Mellanox ConnectX-7 400GbE ASIC",      "Networking",       580, 21),
    ("P060","Xilinx Alveo U55C Data Center Card",   "Networking",      5500, 45),
    # Wafers & Equipment
    ("P061","TSMC 3nm N3E Wafer Batch 25pc",        "Wafers",         65000, 90),
    ("P062","TSMC 5nm N5P Wafer Batch 25pc",        "Wafers",         38000, 75),
    ("P063","TSMC 7nm N7+ Wafer Batch 25pc",        "Wafers",         22000, 60),
    ("P064","Samsung 4nm SF4 Wafer Batch",          "Wafers",         42000, 80),
    ("P065","GlobalFoundries 12nm FinFET Wafer",    "Wafers",          8500, 55),
    ("P066","ASML EUV Lens Module OAI-6XX",         "Wafers",         95000,180),
    ("P067","ASML DUV ArF Immersion Optics Kit",    "Wafers",         45000,120),
    ("P068","Applied Materials CVD Chamber Kit",    "Wafers",         28000, 90),
    ("P069","Lam Research ALD Precursor Kit",       "Wafers",         12000, 45),
    ("P070","Entegris UHP Isopropanol 1000L",       "Wafers",          3800, 14),
    # SoC / Processors
    ("P071","Qualcomm Snapdragon X Elite X1E-80",   "SoC",              195, 21),
    ("P072","AMD EPYC Genoa 9654 96C Processor",    "SoC",            11200, 28),
    ("P073","Intel Xeon Platinum 8592+ Processor",  "SoC",             9800, 25),
    ("P074","Ampere Altra Max 128C Processor",      "SoC",             4500, 30),
    ("P075","RISC-V SiFive P870 Processor",         "SoC",              420, 35),
    ("P076","NXP LPC55S69 Secure MCU",              "SoC",               12, 10),
    ("P077","STM32H7 Cortex-M7 MCU",               "SoC",                9, 10),
    ("P078","Raspberry Pi CM4 Compute Module",      "SoC",               35, 14),
    ("P079","Apple M4 Pro Chip OEM Module",         "SoC",              380, 45),
    ("P080","AWS Graviton4 OEM Module",             "SoC",             2800, 60),
    # Industrial & IoT
    ("P081","Siemens S7-1500 CPU 1518 PLC Module",  "Industrial",      2800, 30),
    ("P082","ABB ACS880 Industrial Drive Module",   "Industrial",      3500, 35),
    ("P083","Rockwell ControlLogix MCU",            "Industrial",      1850, 25),
    ("P084","Phoenix Contact IIoT Gateway FL mRC",  "Industrial",       680, 18),
    ("P085","Beckhoff EtherCAT Slave Controller",   "Industrial",       145, 14),
    ("P086","Keyence CV-X480 Machine Vision System","Industrial",      4200, 28),
    ("P087","SICK LiDAR MRS6000 3D Scanner",        "Industrial",      3800, 25),
    ("P088","Cognex In-Sight 9000 Vision System",   "Industrial",      5600, 30),
    ("P089","Honeywell Experion PKS DCS Module",    "Industrial",      2200, 28),
    ("P090","OMRON NJ-Series CPU Industrial Robot", "Industrial",      1950, 22),
    # Telecom
    ("P091","Ericsson AIR 6449 5G Radio Unit",      "Telecom",         4800, 45),
    ("P092","Nokia AirScale 5G Baseband Unit",      "Telecom",         6200, 50),
    ("P093","Qualcomm FSM100xx 5G RAN Chip",        "Telecom",          820, 28),
    ("P094","Marvell OCTEON 10 DPU 5G",             "Telecom",         2400, 35),
    ("P095","Intel FlexRAN 5G BBU Processing Kit",  "Telecom",         3800, 40),
    ("P096","Analog Devices ADRV9009 5G RF Chip",   "Telecom",          580, 25),
    ("P097","Xilinx RFSoC ZU48DR 5G Platform",      "Telecom",         3200, 38),
    ("P098","Ericsson Baseband 6630 Module",         "Telecom",         8500, 55),
    ("P099","Nokia ReefShark 5G Chipset",            "Telecom",         1800, 32),
    ("P100","Huawei Kirin 990 5G RF Module",         "Telecom",          950, 30),
]

# (supplier_id, name, country, region, reliability_score, avg_lead_days, risk_tier)
SUPPLIERS = [
    ("S001","TSMC",                "Taiwan",       "APAC",     95, 70, "LOW",    "Tier-1 foundry. Long-term capacity agreements in place."),
    ("S002","Samsung Foundry",     "South Korea",  "APAC",     88, 65, "LOW",    "Strong yields on 4nm. Gate-all-around transition monitored."),
    ("S003","GlobalFoundries",     "USA",          "Americas", 84, 55, "MEDIUM", "Mature nodes only. No EUV. Suitable for automotive MCUs."),
    ("S004","SMIC",                "China",        "APAC",     62, 80, "HIGH",   "Subject to US export controls. Backup fab only for non-EAR items."),
    ("S005","Intel Foundry",       "USA",          "Americas", 71, 60, "MEDIUM", "18A process ramp delayed. Monitor yield improvement closely."),
    ("S006","Tower Semiconductor", "Israel",       "EMEA",     80, 45, "MEDIUM", "Analog/RF specialty. Regional conflict risk — dual-source recommended."),
    ("S007","Samsung Memory",      "South Korea",  "APAC",     91, 30, "LOW",    "HBM3E ramp ahead of schedule. Priority allocation secured."),
    ("S008","SK Hynix",            "South Korea",  "APAC",     93, 32, "LOW",    "Primary HBM supplier. Co-packaged optics roadmap strong."),
    ("S009","Micron Technology",   "USA",          "Americas", 87, 28, "LOW",    "US-based memory. CHIPS Act beneficiary. Stable supply."),
    ("S010","KIOXIA",              "Japan",        "APAC",     79, 35, "MEDIUM", "NAND specialist. IPO volatility adds financial risk."),
    ("S011","Nvidia Corporation",  "USA",          "Americas", 85, 42, "LOW",    "GPU allocation tight due to AI demand surge. Lead times extended."),
    ("S012","AMD",                 "USA",          "Americas", 82, 35, "LOW",    "MI300X supply improving. TSMC dependency noted."),
    ("S013","Intel Corporation",   "USA",          "Americas", 78, 30, "MEDIUM", "Foundry transition ongoing. Gaudi ramp stable."),
    ("S014","Bosch Semiconductors","Germany",      "EMEA",     90, 25, "LOW",    "Automotive-grade quality. IATF 16949 certified."),
    ("S015","Mobileye",            "Israel",       "EMEA",     86, 32, "MEDIUM", "Jerusalem HQ. Regional tensions require logistics monitoring."),
    ("S016","Texas Instruments",   "USA",          "Americas", 88, 22, "LOW",    "12 global fabs. Self-sufficient. Low geopolitical risk."),
    ("S017","Renesas Electronics", "Japan",        "APAC",     83, 20, "LOW",    "Post-fire recovery complete. Inventory rebuild done."),
    ("S018","NXP Semiconductors",  "Netherlands",  "EMEA",     87, 24, "LOW",    "Strong in automotive networking. TSMC/Samsung dual source."),
    ("S019","Infineon Technologies","Germany",     "EMEA",     89, 22, "LOW",    "SiC leadership. Dresden fab expansion 2024."),
    ("S020","STMicroelectronics",  "Switzerland",  "EMEA",     84, 20, "LOW",    "SiC MOSFET capacity doubled. Strong automotive pipeline."),
    ("S021","Wolfspeed",           "USA",          "Americas", 74, 18, "MEDIUM", "SiC pioneer but yield challenges on 200mm. Watch Q3 margins."),
    ("S022","ON Semiconductor",    "USA",          "Americas", 80, 16, "LOW",    "EV power modules. Czech fab online 2024."),
    ("S023","Fuji Electric",       "Japan",        "APAC",     85, 25, "LOW",    "IGBT market leader. Long relationship, consistent quality."),
    ("S024","Broadcom",            "USA",          "Americas", 88, 40, "LOW",    "Network ASICs. Custom AI silicon (XPU) pipeline strong."),
    ("S025","Marvell Technology",  "USA",          "Americas", 84, 35, "LOW",    "Data center silicon. Strong HBM integration roadmap."),
    ("S026","Cisco Systems",       "USA",          "Americas", 82, 38, "LOW",    "Silicon One ASIC. Internal fab strategy maturing."),
    ("S027","ASML",                "Netherlands",  "EMEA",     97, 150,"LOW",    "EUV monopoly. Lead times 12-18 months. Critical long-term contracts."),
    ("S028","Applied Materials",   "USA",          "Americas", 90, 85, "LOW",    "CVD/PVD equipment. China export restrictions: monitor quarterly."),
    ("S029","Lam Research",        "USA",          "Americas", 89, 70, "LOW",    "Etch/deposition. China revenue exposure flagged by legal."),
    ("S030","Entegris",            "USA",          "Americas", 85, 14, "LOW",    "Process chemicals. CMC acquisition integrated."),
    ("S031","Siemens AG",          "Germany",      "EMEA",     91, 28, "LOW",    "Industrial automation. Energy crisis resilience measures in place."),
    ("S032","ABB Ltd",             "Switzerland",  "EMEA",     88, 32, "LOW",    "Robotics and drives. Strong European footprint."),
    ("S033","Rockwell Automation", "USA",          "Americas", 85, 25, "LOW",    "ControlLogix PLC. Expanding into smart manufacturing."),
    ("S034","Keyence Corporation", "Japan",        "APAC",     92, 25, "LOW",    "Vision systems. No debt, strong balance sheet."),
    ("S035","Ericsson",            "Sweden",       "EMEA",     83, 42, "MEDIUM", "5G radio. Revenue under pressure from market share loss."),
    ("S036","Nokia Bell Labs",     "Finland",      "EMEA",     80, 48, "MEDIUM", "5G baseband. Recovery from 2023 restructuring."),
    ("S037","Qualcomm",            "USA",          "Americas", 86, 26, "LOW",    "Snapdragon dominant. Apple dependency resolved with Arm deal."),
    ("S038","Analog Devices",      "USA",          "Americas", 90, 24, "LOW",    "RF/mixed-signal. Maxim integration complete."),
]

# Which supplier provides each product category
_CATEGORY_SUPPLIERS = {
    "AI_Accelerator": ["S011","S012","S013"],
    "Memory":         ["S007","S008","S009","S010"],
    "Automotive":     ["S014","S015","S016","S017","S018","S019","S020"],
    "Power":          ["S019","S020","S021","S022","S023"],
    "Networking":     ["S024","S025","S026"],
    "Wafers":         ["S001","S002","S003","S027","S028","S029","S030"],
    "SoC":            ["S001","S002","S013","S037"],
    "Industrial":     ["S031","S032","S033","S034"],
    "Telecom":        ["S035","S036","S037","S038"],
}


# ── Auth ──────────────────────────────────────────────────────────────────────

def _get_aad_token() -> str:
    for az in _AZ_PATHS:
        try:
            r = subprocess.run(
                [az, "account", "get-access-token",
                 "--resource", _DATABRICKS_RESOURCE,
                 "--query", "accessToken", "-o", "tsv"],
                capture_output=True, text=True, timeout=30,
            )
            if r.returncode == 0 and r.stdout.strip():
                return r.stdout.strip()
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
    raise RuntimeError("Azure CLI not found or not logged in. Run: az login")


def _get_client_and_token():
    host  = os.environ["DATABRICKS_HOST"]
    token = _get_aad_token()
    return WorkspaceClient(host=host, token=token), token


# ── SQL execution ─────────────────────────────────────────────────────────────

def _run_sql(w: WorkspaceClient, stmt: str, label: str, wait: int = 50,
             retries: int = 3) -> None:
    wh = os.environ["DATABRICKS_WAREHOUSE_ID"]
    for attempt in range(retries):
        resp = w.statement_execution.execute_statement(
            warehouse_id=wh, statement=stmt, wait_timeout=f"{wait}s",
        )
        state = resp.status.state
        if state in (StatementState.PENDING, StatementState.RUNNING):
            for _ in range(40):
                time.sleep(5)
                resp  = w.statement_execution.get_statement(resp.statement_id)
                state = resp.status.state
                if state not in (StatementState.PENDING, StatementState.RUNNING):
                    break
        if state == StatementState.SUCCEEDED:
            return
        err_msg = (resp.status.error.message if resp.status.error else "unknown")
        # Retry on transient Delta concurrency conflicts
        if "DELTA_METADATA_CHANGED" in err_msg and attempt < retries - 1:
            print(f"  [RETRY {attempt+1}] {label} — metadata conflict, retrying ...", flush=True)
            time.sleep(3)
            continue
        raise RuntimeError(f"{label} FAILED [{state}]: {err_msg}")
    print(f"  [OK] {label}", flush=True)


def _esc(s: str) -> str:
    return str(s).replace("'", "''")


def _insert_chunks(w, table_fqn, rows_sql: list[str], chunk: int, label: str) -> None:
    for i in range(0, len(rows_sql), chunk):
        batch = rows_sql[i:i+chunk]
        stmt  = f"INSERT INTO {table_fqn} VALUES\n" + ",\n".join(batch)
        _run_sql(w, stmt, f"{label} rows {i+1}-{i+len(batch)}")


# ── Data generation ───────────────────────────────────────────────────────────

def _generate_suppliers() -> list[str]:
    rows = []
    for sid, name, country, region, rel, lt, tier, notes in SUPPLIERS:
        rows.append(
            f"('{sid}','{_esc(name)}','{country}','{region}',"
            f"{rel},{lt},'{tier}','{_esc(notes)}')"
        )
    return rows


def _generate_inventory() -> list[str]:
    product_map = {p[0]: p for p in PRODUCTS}
    rows = []
    inv_id = 1
    # Stock 5 of the 8 warehouses per product (randomised but seeded)
    wh_pool = [w[0] for w in WAREHOUSES]
    for pid, name, cat, cost, lt in PRODUCTS:
        whs = RAND.sample(wh_pool, 5)
        sup = RAND.choice(_CATEGORY_SUPPLIERS.get(cat, ["S001"]))
        for wh_id in whs:
            wh_location, wh_region = next(
                (w[1], w[2]) for w in WAREHOUSES if w[0] == wh_id
            )
            # Realistic stock: high-cost items have lower stock, cheaper items higher
            base_stock   = max(5,  int(5000 / max(cost, 1)))
            safety_stock = max(2,  int(base_stock * 0.3))
            reorder_pt   = max(3,  int(base_stock * 0.4))
            max_stock    = int(base_stock * 2.5)

            # Inject ~12% at-risk items (ratio < 1.2)
            if RAND.random() < 0.12:
                stock = int(safety_stock * RAND.uniform(0.7, 1.15))
            else:
                stock = int(base_stock * RAND.uniform(0.8, 2.0))
            stock = max(1, stock)

            rows.append(
                f"('INV{inv_id:04d}','{pid}','{_esc(name)}','{cat}','{sup}',"
                f"'{pid}-{wh_id}','{wh_id}','{wh_location}','{wh_region}',"
                f"{stock},{safety_stock},{reorder_pt},{max_stock},"
                f"{cost},{lt},current_timestamp())"
            )
            inv_id += 1
    return rows


def _generate_orders(n: int = 20_000) -> list[str]:
    start = date(2024, 1, 1)
    end   = date(2025, 12, 31)
    days  = (end - start).days

    statuses  = ["PENDING","PROCESSING","SHIPPED","DELIVERED","DELIVERED","DELIVERED","CANCELLED"]
    priorities= ["LOW","MEDIUM","MEDIUM","HIGH","URGENT"]
    regions   = ["Europe","Americas","APAC","EMEA","Americas","APAC","APAC"]
    customers = [f"CUST{i:04d}" for i in range(1, 501)]

    rows = []
    for i in range(1, n+1):
        pid, name, cat, cost, _ = RAND.choice(PRODUCTS)
        sup = RAND.choice(_CATEGORY_SUPPLIERS.get(cat, ["S001"]))
        qty = int(RNG.integers(1, 20))
        unit_price = round(cost * RAND.uniform(1.05, 1.35), 2)
        total = round(qty * unit_price, 2)
        d = start + timedelta(days=int(RNG.integers(0, days)))
        status = RAND.choice(statuses)
        prio   = RAND.choice(priorities)
        cust   = RAND.choice(customers)
        creg   = RAND.choice(regions)
        dreg   = RAND.choice(regions)
        rows.append(
            f"('ORD{i:06d}','{pid}','{sup}','{cust}','{creg}',"
            f"{qty},{unit_price},{total},'{status}','{prio}',"
            f"'{d}',{d.year},{d.month},'{dreg}')"
        )
    return rows


def _generate_shipments(n: int = 8_000) -> list[str]:
    carriers = ["DHL Express","FedEx Supply Chain","UPS Supply Chain",
                "Kuehne+Nagel","DB Schenker","Maersk","CEVA Logistics","XPO Logistics"]
    statuses = ["IN_TRANSIT","IN_TRANSIT","DELIVERED","DELIVERED","DELIVERED","DELAYED","LOST"]
    start = date(2024, 1, 15)

    rows = []
    for i in range(1, n+1):
        oid  = f"ORD{RAND.randint(1, 20000):06d}"
        orig = RAND.choice(WAREHOUSES)[1]
        dest = RAND.choice(["Europe","Americas","APAC","EMEA"])
        carr = RAND.choice(carriers)
        ship_d = start + timedelta(days=int(RNG.integers(0, 700)))
        transit= int(RNG.integers(3, 30))
        est_d  = ship_d + timedelta(days=transit)
        status = RAND.choice(statuses)
        delay  = 0
        actual = "null"
        if status == "DELIVERED":
            extra  = int(RNG.integers(0, 5))
            actual = f"'{est_d + timedelta(days=extra)}'"
            delay  = extra
        elif status == "DELAYED":
            delay  = int(RNG.integers(5, 30))
        tracking = f"TRK{RAND.randint(10**9, 10**10-1)}"
        rows.append(
            f"('SHP{i:06d}','{oid}','{carr}','{orig}','{dest}',"
            f"'{ship_d}','{est_d}',{actual},'{status}',{delay},'{tracking}')"
        )
    return rows


# VARIANT showcase: each event_type carries a completely different JSON schema
def _generate_disruptions(n: int = 300) -> list[str]:
    event_types = {
        "GEOPOLITICAL": lambda: {
            "description": RAND.choice([
                "Taiwan Strait tensions affecting semiconductor shipments",
                "US-China trade restrictions on advanced chips",
                "EU semiconductor sovereignty act implementation",
                "South Korea-Japan export dispute on fluorinated gas",
                "India CHIPS Act reshaping APAC supply routes",
            ]),
            "affected_ports": RAND.sample(["Kaohsiung","Busan","Singapore","Rotterdam","Long Beach"], RAND.randint(1,3)),
            "tariff_change_pct": round(RAND.uniform(-5, 35), 1),
            "country_pair": RAND.choice(["US-China","US-Taiwan","EU-China","Japan-Korea"]),
            "diplomatic_status": RAND.choice(["escalating","stable","de-escalating","sanctions_imposed"]),
        },
        "NATURAL_DISASTER": lambda: {
            "description": RAND.choice([
                "Typhoon Mawar disrupts Taipei and Kaohsiung manufacturing",
                "Magnitude 7.4 earthquake near Hsinchu Science Park",
                "Texas winter storm shuts Austin fab for 8 days",
                "Flooding in Zhengzhou affects Foxconn assembly lines",
                "Drought reduces Rhine River logistics capacity by 40%",
            ]),
            "disaster_type": RAND.choice(["typhoon","earthquake","flood","winter_storm","drought"]),
            "magnitude_or_category": round(RAND.uniform(3.5, 9.0), 1),
            "affected_area_km2": int(RAND.uniform(500, 50000)),
            "factory_downtime_days": int(RAND.uniform(2, 45)),
            "estimated_loss_usd": int(RAND.uniform(1e6, 5e8)),
        },
        "SUPPLIER_FAILURE": lambda: {
            "description": RAND.choice([
                "TSMC N3 node yield degradation below 60% target",
                "Samsung HBM3 contamination event halts Line 17",
                "ASML EUV machine breakdown at customer site",
                "Entegris chemical purity failure batch recall",
                "Wolfspeed SiC substrate defect across Q3 lots",
            ]),
            "failure_type": RAND.choice(["yield_degradation","contamination","equipment_breakdown","quality_recall","process_drift"]),
            "affected_process_node": RAND.choice(["3nm","4nm","5nm","7nm","12nm","28nm","SiC-150mm"]),
            "yield_impact_pct": round(RAND.uniform(-40, -5), 1),
            "recovery_timeline_weeks": int(RAND.uniform(2, 24)),
            "batches_affected": int(RAND.uniform(5, 500)),
        },
        "LOGISTICS_DISRUPTION": lambda: {
            "description": RAND.choice([
                "Suez Canal blocked by vessel grounding — 6 day delay",
                "Panama Canal low water levels reduce daily transits by 50%",
                "Air cargo capacity shortage due to aviation fuel crisis",
                "Port of Rotterdam strike — container backlog 3 weeks",
                "Trans-Pacific shipping rate spike 180% YoY",
            ]),
            "route": RAND.choice(["Suez_Canal","Panama_Canal","Trans_Pacific","Asia_Europe","Trans_Atlantic"]),
            "affected_port": RAND.choice(["Rotterdam","Singapore","Long Beach","Shanghai","Kaohsiung","Busan"]),
            "delay_days": int(RAND.uniform(3, 45)),
            "vessels_affected": int(RAND.uniform(10, 400)),
            "rate_increase_pct": round(RAND.uniform(20, 220), 1),
        },
        "CYBER_ATTACK": lambda: {
            "description": RAND.choice([
                "Ransomware attack on Tier-1 supplier ERP system",
                "Nation-state intrusion in fab SCADA network",
                "Supply chain malware in firmware update pipeline",
                "DDoS attack on logistics tracking platform",
                "Data breach at contract manufacturer — IP theft",
            ]),
            "attack_type": RAND.choice(["ransomware","nation_state","supply_chain_malware","ddos","data_breach"]),
            "target_system": RAND.choice(["ERP","SCADA","MES","WMS","logistics_portal","firmware_pipeline"]),
            "downtime_hours": int(RAND.uniform(2, 720)),
            "data_breach": RAND.choice([True, False, False]),
            "ransom_demanded_usd": int(RAND.uniform(0, 50_000_000)),
        },
        "REGULATORY": lambda: {
            "description": RAND.choice([
                "US BIS Entity List addition of 12 Chinese semiconductor firms",
                "EU Carbon Border Adjustment Mechanism affects chip imports",
                "CHIPS Act Section 22 restricts Chinese joint ventures",
                "Japan tightens export controls on semiconductor equipment",
                "UK NSIA review blocks foreign acquisition of UK fab",
            ]),
            "regulation": RAND.choice(["EAR_Entity_List","EU_CBAM","CHIPS_Act_Sec22","Japan_Export_Control","UK_NSIA"]),
            "jurisdiction": RAND.choice(["USA","EU","Japan","UK","Netherlands"]),
            "compliance_deadline_days": int(RAND.uniform(30, 365)),
            "penalty_usd_max": int(RAND.uniform(100_000, 10_000_000)),
            "affected_supplier_count": int(RAND.uniform(1, 50)),
        },
    }

    severities   = ["CRITICAL","HIGH","HIGH","MEDIUM","MEDIUM","LOW"]
    categories_p = list(PRODUCTS)
    all_regions  = [w[2] for w in WAREHOUSES]

    rows = []
    event_start = date(2024, 1, 1)
    event_end   = date(2025, 12, 31)
    days_range  = (event_end - event_start).days

    for i in range(1, n+1):
        etype = RAND.choice(list(event_types.keys()))
        details = event_types[etype]()
        sev    = RAND.choice(severities)
        impact = round(RAND.uniform(1.0, 10.0), 2)
        resolved = RAND.random() > 0.35
        edate  = event_start + timedelta(days=int(RNG.integers(0, days_range)))
        res_date = "null"
        if resolved:
            res_date = f"'{edate + timedelta(days=int(RNG.integers(7,120)))}'"

        # Affected regions and categories (comma-separated for simplicity)
        aff_reg  = ",".join(RAND.sample(list(dict.fromkeys(all_regions)), RAND.randint(1,3)))
        aff_cats = ",".join(RAND.sample(
            list({p[2] for p in categories_p}), RAND.randint(1,3)
        ))
        details_json = _esc(json.dumps(details, ensure_ascii=False))

        rows.append(
            f"('EVT{i:04d}','{edate}','{etype}','{sev}',"
            f"'{aff_reg}','{aff_cats}',"
            f"{impact},{str(resolved).lower()},{res_date},"
            f"parse_json('{details_json}'))"
        )
    return rows


# ── Schema creation ───────────────────────────────────────────────────────────

def _create_tables(w: WorkspaceClient) -> None:
    print("\n[SCHEMA] Creating Delta tables ...", flush=True)

    ddls = [
        # 1. suppliers
        (f"""
        CREATE OR REPLACE TABLE {FQN}.suppliers (
            supplier_id        STRING    NOT NULL,
            supplier_name      STRING    NOT NULL,
            country            STRING    NOT NULL,
            region             STRING    NOT NULL,
            reliability_score  INT,
            avg_lead_time_days INT,
            risk_tier          STRING,
            risk_notes         STRING
        ) USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """, "CREATE suppliers"),

        # 2. inventory — ZORDER + CDF
        (f"""
        CREATE OR REPLACE TABLE {FQN}.inventory (
            inventory_id        STRING     NOT NULL,
            product_id          STRING     NOT NULL,
            product_name        STRING     NOT NULL,
            category            STRING     NOT NULL,
            supplier_id         STRING     NOT NULL,
            sku                 STRING     NOT NULL,
            warehouse_id        STRING     NOT NULL,
            warehouse_location  STRING     NOT NULL,
            region              STRING     NOT NULL,
            stock_level         INT        NOT NULL,
            safety_stock        INT        NOT NULL,
            reorder_point       INT        NOT NULL,
            max_stock           INT        NOT NULL,
            unit_cost           DOUBLE     NOT NULL,
            lead_time_days      INT        NOT NULL,
            last_updated        TIMESTAMP  NOT NULL
        ) USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
        """, "CREATE inventory"),

        # 3. orders — PARTITIONED BY (year, month)
        (f"""
        CREATE OR REPLACE TABLE {FQN}.orders (
            order_id           STRING  NOT NULL,
            product_id         STRING  NOT NULL,
            supplier_id        STRING  NOT NULL,
            customer_id        STRING  NOT NULL,
            customer_region    STRING  NOT NULL,
            quantity           INT     NOT NULL,
            unit_price         DOUBLE  NOT NULL,
            total_value        DOUBLE  NOT NULL,
            order_status       STRING  NOT NULL,
            priority           STRING  NOT NULL,
            order_date         DATE    NOT NULL,
            order_year         INT     NOT NULL,
            order_month        INT     NOT NULL,
            delivery_region    STRING
        ) USING DELTA
        PARTITIONED BY (order_year, order_month)
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """, "CREATE orders"),

        # 4. shipments — PARTITIONED BY status
        (f"""
        CREATE OR REPLACE TABLE {FQN}.shipments (
            shipment_id           STRING  NOT NULL,
            order_id              STRING  NOT NULL,
            carrier               STRING  NOT NULL,
            origin_warehouse      STRING  NOT NULL,
            destination_region    STRING  NOT NULL,
            ship_date             DATE    NOT NULL,
            est_delivery_date     DATE    NOT NULL,
            actual_delivery_date  DATE,
            status                STRING  NOT NULL,
            delay_days            INT,
            tracking_number       STRING
        ) USING DELTA
        PARTITIONED BY (status)
        """, "CREATE shipments"),

        # 5. disruption_events — VARIANT column (the key differentiator)
        (f"""
        CREATE OR REPLACE TABLE {FQN}.disruption_events (
            event_id            STRING   NOT NULL,
            event_date          DATE     NOT NULL,
            event_type          STRING   NOT NULL,
            severity            STRING   NOT NULL,
            affected_regions    STRING,
            affected_categories STRING,
            impact_score        DOUBLE,
            resolved            BOOLEAN,
            resolution_date     DATE,
            event_details       VARIANT
        ) USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """, "CREATE disruption_events"),
    ]

    for ddl, label in ddls:
        _run_sql(w, ddl.strip(), label)


# ── Post-load optimisation ────────────────────────────────────────────────────

def _optimize(w: WorkspaceClient) -> None:
    print("\n[OPTIMIZE] Running ZORDER and OPTIMIZE ...", flush=True)
    _run_sql(w,
        f"OPTIMIZE {FQN}.inventory ZORDER BY (product_id, warehouse_id)",
        "ZORDER inventory")
    _run_sql(w,
        f"OPTIMIZE {FQN}.orders ZORDER BY (product_id, customer_region)",
        "ZORDER orders")
    _run_sql(w,
        f"ANALYZE TABLE {FQN}.inventory COMPUTE STATISTICS FOR ALL COLUMNS",
        "ANALYZE inventory")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 65, flush=True)
    print("  AI Supply Chain — Databricks Delta Lake Seed v2", flush=True)
    print("=" * 65 + "\n", flush=True)

    print("[AUTH] Obtaining Azure AD token ...", flush=True)
    w, _token = _get_client_and_token()
    print("[AUTH] Connected.\n", flush=True)

    # ── 1. Create tables ──────────────────────────────────────────────────────
    print("STEP 1/4  Create Delta tables", flush=True)
    print("-" * 45, flush=True)
    _create_tables(w)

    # ── 2. Generate data ──────────────────────────────────────────────────────
    print("\nSTEP 2/4  Generate data", flush=True)
    print("-" * 45, flush=True)

    print("  Generating suppliers  (38 rows) ...",   end=" ", flush=True)
    sup_rows = _generate_suppliers();  print("done")

    print("  Generating inventory  (500 rows) ...",  end=" ", flush=True)
    inv_rows = _generate_inventory();  print("done")

    print("  Generating orders     (20 000 rows) ...", end=" ", flush=True)
    ord_rows = _generate_orders(20_000); print("done")

    print("  Generating shipments  (8 000 rows) ...", end=" ", flush=True)
    shp_rows = _generate_shipments(8_000); print("done")

    print("  Generating disruption_events (300 rows, VARIANT) ...", end=" ", flush=True)
    dis_rows = _generate_disruptions(300); print("done")

    # ── 3. Insert data ────────────────────────────────────────────────────────
    print("\nSTEP 3/4  Insert data", flush=True)
    print("-" * 45, flush=True)

    # Truncate all tables before re-inserting (idempotent re-runs)
    print("  Truncating tables for clean load ...", flush=True)
    for t in ("suppliers","inventory","orders","shipments","disruption_events"):
        _run_sql(w, f"TRUNCATE TABLE {FQN}.{t}", f"TRUNCATE {t}")

    print(f"\n  [suppliers — {len(sup_rows)} rows]", flush=True)
    _insert_chunks(w, f"{FQN}.suppliers", sup_rows, 38, "suppliers")

    print(f"\n  [inventory — {len(inv_rows)} rows]", flush=True)
    _insert_chunks(w, f"{FQN}.inventory", inv_rows, 100, "inventory")

    print(f"\n  [orders — {len(ord_rows)} rows, 200 rows/batch]", flush=True)
    _insert_chunks(w, f"{FQN}.orders",    ord_rows, 200, "orders")

    print(f"\n  [shipments — {len(shp_rows)} rows, 200 rows/batch]", flush=True)
    _insert_chunks(w, f"{FQN}.shipments", shp_rows, 200, "shipments")

    print(f"\n  [disruption_events — {len(dis_rows)} rows, VARIANT]", flush=True)
    _insert_chunks(w, f"{FQN}.disruption_events", dis_rows, 50, "disruption_events")

    # ── 4. Optimise ────────────────────────────────────────────────────────────
    print("\nSTEP 4/4  Optimise", flush=True)
    print("-" * 45, flush=True)
    _optimize(w)

    # ── Summary ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 65, flush=True)
    print("  Done! Tables in db_supply_chain_workspace.supply_chain_data:", flush=True)
    rows_map = {
        "suppliers": len(sup_rows),
        "inventory": len(inv_rows),
        "orders":    len(ord_rows),
        "shipments": len(shp_rows),
        "disruption_events": len(dis_rows),
    }
    for t, n in rows_map.items():
        print(f"    {t:<25} {n:>6} rows", flush=True)
    total = sum(rows_map.values())
    print(f"    {'TOTAL':<25} {total:>6} rows", flush=True)
    print("=" * 65, flush=True)
    print("\nDelta features enabled:", flush=True)
    print("  VARIANT           — disruption_events.event_details", flush=True)
    print("  PARTITIONED BY    — orders (year/month), shipments (status)", flush=True)
    print("  ZORDER BY         — inventory (product_id, warehouse_id)", flush=True)
    print("  Change Data Feed  — all 5 tables", flush=True)
    print("\nTry these queries in Databricks SQL Editor:", flush=True)
    print(f"  SELECT event_details:description::STRING, event_details:tariff_change_pct::DOUBLE", flush=True)
    print(f"  FROM {FQN}.disruption_events WHERE event_type='GEOPOLITICAL';", flush=True)
    print(f"\n  SELECT * FROM {FQN}.orders VERSION AS OF 0;  -- time travel", flush=True)
    print(f"\n  SELECT * FROM table_changes('{FQN}.inventory', 0);  -- CDC", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\n[ERROR] {exc}", flush=True)
        traceback.print_exc()
        sys.exit(1)
