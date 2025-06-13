/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
  global_start TIMESTAMP := now();
  global_end TIMESTAMP;
  t_start TIMESTAMP;
  t_end TIMESTAMP;
  row_count BIGINT;
BEGIN
  RAISE NOTICE '========= START: Bronze Layer Loaded at % =========', global_start;

  -- CRM: Customer Info
  t_start := clock_timestamp();
  TRUNCATE TABLE bronze.crm_cust_info;
  COPY bronze.crm_cust_info
  FROM 'C:/postgres_shared/datasets/source_crm/cust_info.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');
  t_end := clock_timestamp();
  Select count(*) INTO row_count FROM bronze.crm_cust_info;
  RAISE NOTICE 'Loaded crm_cust_info in % seconds. Row count: %', ROUND(EXTRACT(EPOCH FROM t_end - t_start)::numeric, 2), row_count;

  -- CRM: Product Info
  t_start := clock_timestamp();
  TRUNCATE TABLE bronze.crm_prd_info;
  COPY bronze.crm_prd_info
  FROM 'C:/postgres_shared/datasets/source_crm/prd_info.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');
  t_end := clock_timestamp();
  Select count(*) INTO row_count FROM bronze.crm_prd_info;
  RAISE NOTICE 'Loaded crm_prd_info in % seconds. Row count: %', ROUND(EXTRACT(EPOCH FROM t_end - t_start)::numeric, 2), row_count;

  -- CRM: Sales Details
  t_start := clock_timestamp();
  TRUNCATE TABLE bronze.crm_sales_details;
  COPY bronze.crm_sales_details
  FROM 'C:/postgres_shared/datasets/source_crm/sales_details.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');
  t_end := clock_timestamp();
  Select count(*) INTO row_count FROM bronze.crm_sales_details;
  RAISE NOTICE 'Loaded crm_prd_info in % seconds. Row count: %', ROUND(EXTRACT(EPOCH FROM t_end - t_start)::numeric, 2), row_count;

  -- ERP: Location Info
  t_start := clock_timestamp();
  TRUNCATE TABLE bronze.erp_loc_a101;
  COPY bronze.erp_loc_a101
  FROM 'C:/postgres_shared/datasets/source_erp/LOC_A101.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');
  t_end := clock_timestamp();
  Select count(*) INTO row_count FROM bronze.erp_loc_a101;
  RAISE NOTICE 'Loaded crm_prd_info in % seconds. Row count: %', ROUND(EXTRACT(EPOCH FROM t_end - t_start)::numeric, 2), row_count;

  -- ERP: Customer AZ12
  t_start := clock_timestamp();
  TRUNCATE TABLE bronze.erp_cust_az12;
  COPY bronze.erp_cust_az12
  FROM 'C:/postgres_shared/datasets/source_erp/CUST_AZ12.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');
  t_end := clock_timestamp();
  Select count(*) INTO row_count FROM bronze.erp_loc_a101;
  RAISE NOTICE 'Loaded crm_prd_info in % seconds. Row count: %', ROUND(EXTRACT(EPOCH FROM t_end - t_start)::numeric, 2), row_count; 

  -- ERP: Product Category G1V2
  t_start := clock_timestamp();
  TRUNCATE TABLE bronze.erp_px_cat_g1v2;
  COPY bronze.erp_px_cat_g1v2
  FROM 'C:/postgres_shared/datasets/source_erp/PX_CAT_G1V2.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER ',');
  t_end := clock_timestamp();
  RAISE NOTICE 'Loaded erp_px_cat_g1v2 in % seconds', ROUND(EXTRACT(EPOCH FROM t_end - t_start)::numeric, 2);

  -- Final Logging
  global_end := clock_timestamp();
  RAISE NOTICE '======== SUCCESS: Bronze layer loaded at % ========', global_end;
  RAISE NOTICE 'Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM global_end - global_start)::numeric, 2);

EXCEPTION
  WHEN OTHERS THEN
    RAISE NOTICE '========== ERROR OCCURRED DURING LOADING BRONZE LAYER =========='; 
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'SQLSTATE Code: %', SQLSTATE;
END;
$$;