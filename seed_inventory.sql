-- Auto-generated fallback: db_supply_chain_workspace.supply_chain_data.inventory
-- 15 rows  |  run in Databricks SQL Editor or CLI

CREATE TABLE IF NOT EXISTS db_supply_chain_workspace.supply_chain_data.inventory (
    product_id          STRING  NOT NULL,
    product_name        STRING  NOT NULL,
    stock_level         INT     NOT NULL,
    safety_stock        INT     NOT NULL,
    warehouse_location  STRING  NOT NULL
) USING DELTA;

INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P001', 'Nvidia H100 SXM5 80GB GPU', 45, 15, 'Stuttgart');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P002', 'Nvidia A100 PCIe 40GB GPU', 80, 30, 'Singapore');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P003', 'Bosch LiDAR Sensor LRR4', 250, 100, 'Austin TX');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P004', 'Bosch Radar Sensor SRR3', 320, 120, 'Shenzhen');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P005', 'TSMC 3nm Wafer Batch', 12, 5, 'Seoul');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P006', 'Samsung HBM3 Memory Stack', 180, 60, 'Taipei');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P007', 'ASML EUV Lens Module', 2, 1, 'Munich');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P008', 'Qualcomm Snapdragon X Elite SoC', 150, 50, 'Santa Clara');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P009', 'Intel Gaudi 3 AI Accelerator', 60, 20, 'Stuttgart');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P010', 'Mobileye EyeQ6H Vision Chip', 400, 150, 'Singapore');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P011', 'Infineon IGBT Power Module', 480, 180, 'Austin TX');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P012', 'STMicro SiC MOSFET 1200V', 350, 130, 'Shenzhen');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P013', 'Texas Instruments TDA4VM SoC', 200, 70, 'Seoul');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P014', 'Renesas RH850 Automotive MCU', 280, 100, 'Taipei');
INSERT INTO db_supply_chain_workspace.supply_chain_data.inventory VALUES ('P015', 'AMD Instinct MI300X GPU', 70, 25, 'Munich');
