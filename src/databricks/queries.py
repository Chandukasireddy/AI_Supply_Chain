"""Pre-built SQL queries against the supply chain schema."""
from __future__ import annotations

import pandas as pd
from src.databricks.client import DataClient


def get_low_stock_items(client: DataClient, threshold_factor: float = 1.2) -> pd.DataFrame:
    """Items at or below reorder_point * threshold_factor."""
    return client.query(f"""
        SELECT
            p.product_id,
            p.name,
            p.category,
            i.warehouse_id,
            i.quantity,
            i.reorder_point,
            ROUND(i.reorder_point * {threshold_factor}, 0) AS alert_threshold,
            p.lead_time_days
        FROM inventory i
        JOIN products p ON i.product_id = p.product_id
        WHERE i.quantity <= i.reorder_point * {threshold_factor}
        ORDER BY (i.quantity * 1.0 / i.reorder_point) ASC
    """)


def get_pending_orders(client: DataClient) -> pd.DataFrame:
    """All orders with status pending or in_transit."""
    return client.query("""
        SELECT
            o.order_id,
            p.name        AS product,
            s.name        AS supplier,
            o.quantity,
            o.status,
            o.order_date,
            o.expected_delivery
        FROM orders o
        JOIN products  p ON o.product_id  = p.product_id
        JOIN suppliers s ON o.supplier_id = s.supplier_id
        WHERE o.status IN ('pending', 'in_transit')
        ORDER BY o.expected_delivery ASC
    """)


def get_demand_forecast(client: DataClient, days_ahead: int = 30) -> pd.DataFrame:
    return client.query(f"""
        SELECT
            p.name,
            p.category,
            df.forecast_date,
            df.predicted_demand,
            df.confidence
        FROM demand_forecast df
        JOIN products p ON df.product_id = p.product_id
        WHERE df.forecast_date BETWEEN CURRENT_DATE
              AND CURRENT_DATE + INTERVAL '{days_ahead}' DAY
        ORDER BY df.forecast_date, p.name
    """)


def get_supplier_performance(client: DataClient) -> pd.DataFrame:
    return client.query("""
        SELECT
            s.supplier_id,
            s.name,
            s.country,
            s.reliability_score,
            s.avg_delivery_days,
            COUNT(o.order_id)   AS total_orders,
            SUM(o.quantity)     AS total_units_ordered
        FROM suppliers s
        LEFT JOIN orders o ON s.supplier_id = o.supplier_id
        GROUP BY s.supplier_id, s.name, s.country,
                 s.reliability_score, s.avg_delivery_days
        ORDER BY s.reliability_score DESC
    """)


def get_inventory_summary(client: DataClient) -> pd.DataFrame:
    return client.query("""
        SELECT
            p.category,
            COUNT(DISTINCT p.product_id) AS product_count,
            SUM(i.quantity)              AS total_units,
            SUM(i.quantity * p.unit_cost) AS inventory_value
        FROM inventory i
        JOIN products p ON i.product_id = p.product_id
        GROUP BY p.category
        ORDER BY inventory_value DESC
    """)
