-- ── Products ─────────────────────────────────────────────────────────────────
INSERT INTO products VALUES
    ('P001', 'Lithium Battery Pack',    'Electronics',  42.50,  14),
    ('P002', 'Industrial Servo Motor',  'Machinery',   215.00,  21),
    ('P003', 'HDPE Pipe 50mm',          'Materials',     8.75,   7),
    ('P004', 'Control Board v3',        'Electronics',  95.00,  18),
    ('P005', 'Bearing Assembly 6204',   'Machinery',    12.30,   5),
    ('P006', 'Thermal Paste 50g',       'Electronics',   3.20,   3),
    ('P007', 'Stainless Bolt M8x30',    'Fasteners',     0.45,   2),
    ('P008', 'Pneumatic Cylinder 63mm', 'Machinery',    78.90,  10);

-- ── Warehouses ───────────────────────────────────────────────────────────────
INSERT INTO warehouses VALUES
    ('WH-US-01', 'Chicago, IL',     50000),
    ('WH-EU-01', 'Rotterdam, NL',   35000),
    ('WH-AS-01', 'Singapore',       28000);

-- ── Inventory ────────────────────────────────────────────────────────────────
INSERT INTO inventory VALUES
    ('INV-001', 'P001', 'WH-US-01', 120,  200, CURRENT_TIMESTAMP),
    ('INV-002', 'P001', 'WH-EU-01',  45,   80, CURRENT_TIMESTAMP),
    ('INV-003', 'P002', 'WH-US-01',  15,   25, CURRENT_TIMESTAMP),
    ('INV-004', 'P002', 'WH-AS-01',   8,   20, CURRENT_TIMESTAMP),
    ('INV-005', 'P003', 'WH-US-01', 800,  500, CURRENT_TIMESTAMP),
    ('INV-006', 'P004', 'WH-EU-01',  30,   50, CURRENT_TIMESTAMP),
    ('INV-007', 'P005', 'WH-US-01', 350,  300, CURRENT_TIMESTAMP),
    ('INV-008', 'P005', 'WH-AS-01',  10,  150, CURRENT_TIMESTAMP),
    ('INV-009', 'P006', 'WH-US-01', 500,  200, CURRENT_TIMESTAMP),
    ('INV-010', 'P007', 'WH-US-01',9500, 5000, CURRENT_TIMESTAMP),
    ('INV-011', 'P008', 'WH-EU-01',  18,   40, CURRENT_TIMESTAMP);

-- ── Suppliers ────────────────────────────────────────────────────────────────
INSERT INTO suppliers VALUES
    ('SUP-001', 'Apex Electronics Ltd',   'Taiwan',       0.96,  12),
    ('SUP-002', 'Euromovers GmbH',        'Germany',      0.88,  18),
    ('SUP-003', 'FastFix Supply Co.',     'USA',          0.91,   4),
    ('SUP-004', 'Dragon Parts Intl.',     'China',        0.74,  25),
    ('SUP-005', 'Nordic Materials AS',    'Sweden',       0.93,  10);

-- ── Orders ───────────────────────────────────────────────────────────────────
INSERT INTO orders VALUES
    ('ORD-001', 'P001', 'SUP-001', 500, 'in_transit',  CURRENT_DATE - 8,  CURRENT_DATE + 4),
    ('ORD-002', 'P002', 'SUP-002', 50,  'pending',     CURRENT_DATE - 2,  CURRENT_DATE + 19),
    ('ORD-003', 'P004', 'SUP-001', 100, 'pending',     CURRENT_DATE - 1,  CURRENT_DATE + 17),
    ('ORD-004', 'P005', 'SUP-003', 400, 'in_transit',  CURRENT_DATE - 3,  CURRENT_DATE + 2),
    ('ORD-005', 'P008', 'SUP-002', 30,  'pending',     CURRENT_DATE,      CURRENT_DATE + 20),
    ('ORD-006', 'P003', 'SUP-005', 2000,'delivered',   CURRENT_DATE - 15, CURRENT_DATE - 5),
    ('ORD-007', 'P007', 'SUP-003', 10000,'delivered',  CURRENT_DATE - 10, CURRENT_DATE - 6),
    ('ORD-008', 'P002', 'SUP-004', 20,  'pending',     CURRENT_DATE - 5,  CURRENT_DATE + 20);

-- ── Demand forecast (next 30 days) ───────────────────────────────────────────
INSERT INTO demand_forecast VALUES
    ('FC-001', 'P001', CURRENT_DATE + 7,   420, 0.90),
    ('FC-002', 'P001', CURRENT_DATE + 14,  380, 0.85),
    ('FC-003', 'P001', CURRENT_DATE + 21,  450, 0.80),
    ('FC-004', 'P002', CURRENT_DATE + 7,    30, 0.88),
    ('FC-005', 'P002', CURRENT_DATE + 14,   28, 0.84),
    ('FC-006', 'P003', CURRENT_DATE + 7,  1200, 0.95),
    ('FC-007', 'P004', CURRENT_DATE + 7,    60, 0.78),
    ('FC-008', 'P004', CURRENT_DATE + 14,   75, 0.72),
    ('FC-009', 'P005', CURRENT_DATE + 7,   500, 0.91),
    ('FC-010', 'P005', CURRENT_DATE + 14,  520, 0.89),
    ('FC-011', 'P008', CURRENT_DATE + 7,    25, 0.70),
    ('FC-012', 'P008', CURRENT_DATE + 21,   40, 0.65);
