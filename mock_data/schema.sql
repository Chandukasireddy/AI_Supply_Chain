-- Supply-chain mock schema (DuckDB / Databricks SQL compatible)

CREATE TABLE IF NOT EXISTS products (
    product_id      VARCHAR PRIMARY KEY,
    name            VARCHAR NOT NULL,
    category        VARCHAR NOT NULL,
    unit_cost       DECIMAL(10, 2) NOT NULL,
    lead_time_days  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_id    VARCHAR PRIMARY KEY,
    location        VARCHAR NOT NULL,
    capacity        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory (
    inventory_id    VARCHAR PRIMARY KEY,
    product_id      VARCHAR NOT NULL REFERENCES products(product_id),
    warehouse_id    VARCHAR NOT NULL REFERENCES warehouses(warehouse_id),
    quantity        INTEGER NOT NULL CHECK (quantity >= 0),
    reorder_point   INTEGER NOT NULL,
    last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id         VARCHAR PRIMARY KEY,
    name                VARCHAR NOT NULL,
    country             VARCHAR NOT NULL,
    reliability_score   DECIMAL(3, 2) CHECK (reliability_score BETWEEN 0 AND 1),
    avg_delivery_days   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id            VARCHAR PRIMARY KEY,
    product_id          VARCHAR NOT NULL REFERENCES products(product_id),
    supplier_id         VARCHAR NOT NULL REFERENCES suppliers(supplier_id),
    quantity            INTEGER NOT NULL CHECK (quantity > 0),
    status              VARCHAR NOT NULL CHECK (status IN ('pending','in_transit','delivered','cancelled')),
    order_date          DATE NOT NULL,
    expected_delivery   DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS demand_forecast (
    forecast_id         VARCHAR PRIMARY KEY,
    product_id          VARCHAR NOT NULL REFERENCES products(product_id),
    forecast_date       DATE NOT NULL,
    predicted_demand    INTEGER NOT NULL CHECK (predicted_demand >= 0),
    confidence          DECIMAL(3, 2) CHECK (confidence BETWEEN 0 AND 1)
);
