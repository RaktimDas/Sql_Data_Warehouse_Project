/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL Silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
  global_start TIMESTAMP := now();
  global_end TIMESTAMP;
  t_start TIMESTAMP;
  t_end TIMESTAMP;
  row_count BIGINT;
BEGIN
  	RAISE NOTICE '========= START: Silver Layer Loaded at % =========', global_start;

	RAISE NOTICE 'Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	t_start := clock_timestamp();
	INSERT INTO  silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_martial_status,
		cst_gndr,
		cst_create_date
	)
	Select 
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	CASE 
		WHEN UPPER(TRIM(cst_martial_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cst_martial_status)) = 'S' THEN 'Single'
		ELSE 'n/a'
	END cst_martial_status, -- Normalize martial status values to readable fromat
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END cst_gndr, -- Normalize gender values to readable format 
	cst_create_date
	FROM (
	Select *, 
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	From bronze.crm_cust_info
	where cst_id IS NOT NULL
	) as t where flag_last = 1; -- Select the most recent record per 
	t_end := clock_timestamp();
	Select count(*) INTO row_count FROM silver.crm_cust_info;
  	RAISE NOTICE 'Loaded crm_cust_info in % seconds. Row count: %', ROUND(EXTRACT(EPOCH FROM t_end - t_start)::numeric, 2), row_count;
	  
	
	RAISE NOTICE 'Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	t_start := clock_timestamp();
	INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	Select 
		prd_id,
		REPLACE (SUBSTRING(prd_key, 1 , 5), '-','_') as cat_id,
		SUBSTRING(prd_key , 7 , LENGTH(prd_key)) as prd_key,
		prd_nm,
		COALESCE(prd_cost, 0 ) as prd_cost,
		CASE 
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ElSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS  prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(partition by prd_key ORDER BY prd_start_dt)-INTERVAL '1 day' AS DATE) AS prd_end_dt 
	From bronze.crm_prd_info;
	t_end := clock_timestamp();
	Select count(*) INTO row_count FROM silver.crm_prd_info;
  	RAISE NOTICE 'Loaded crm_prd_info in % seconds. Row count: %', ROUND(EXTRACT(EPOCH FROM t_end - t_start)::numeric, 2), row_count;
	
	RAISE NOTICE 'Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	t_start := clock_timestamp();
	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	SELECT 
	  sls_ord_num, 
	  sls_prd_key,
	  sls_cust_id,
	CASE 
	    WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
	    ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
	  END AS sls_order_dt,
	
	CASE 
	    WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
	    ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
	  END AS sls_ship_dt,
	
	CASE 
	    WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
	    ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
	  END AS sls_due_dt,
	
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END as  sls_sales,
	
	sls_quantity,
	CASE 
		WHEN sls_price is NULL OR sls_price <= 0 
			THEN sls_sales / COALESCE(sls_quantity,0)
		ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details;
	t_end := clock_timestamp();
	Select count(*) INTO row_count FROM silver.crm_sales_details;
  	RAISE NOTICE 'Loaded crm_sales_details in % seconds. Row count: %', ROUND(EXTRACT(EPOCH FROM t_end - t_start)::numeric, 2), row_count;
	
	
	RAISE NOTICE 'Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	t_start := clock_timestamp();
	INSERT INTO silver.erp_loc_a101 (cid, cntry)
	Select 
		REPLACE(cid,'-',''),
		CASE 
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
	END AS cntry -- Normalize and Handle missing or blank country codes
	From bronze.erp_loc_a101;
	t_end := clock_timestamp();
	Select count(*) INTO row_count FROM silver.erp_loc_a101;
  	RAISE NOTICE 'Loaded erp_loc_a101 in % seconds. Row count: %', ROUND(EXTRACT(EPOCH FROM t_end - t_start)::numeric, 2), row_count;
	
	RAISE NOTICE 'Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	t_start := clock_timestamp();
	INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
	Select 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) -- Remove 'NAS' prefix if present
		ELSE cid 
	END AS cid,
	CASE WHEN bdate > NOW() THEN NULL 
		ELSE bdate
	END AS bdate, -- Set future birthdates to NULL
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
		ELSE 'n/a'
	END AS gen -- Normalize gender values and handle unknown cases
	FROM bronze.erp_cust_az12;
	t_end := clock_timestamp();
	Select count(*) INTO row_count FROM silver.erp_cust_az12;
  	RAISE NOTICE 'Loaded erp_cust_az12 in % seconds. Row count: %', ROUND(EXTRACT(EPOCH FROM t_end - t_start)::numeric, 2), row_count;
	
	
	RAISE NOTICE 'Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	t_start := clock_timestamp();
	INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	Select 
		id, 
		cat, 
		subcat, 
		maintenance 
	From bronze.erp_px_cat_g1v2;
	t_end := clock_timestamp();
	Select count(*) INTO row_count FROM silver.erp_px_cat_g1v2;
  	RAISE NOTICE 'Loaded erp_px_cat_g1v2 in % seconds. Row count: %', ROUND(EXTRACT(EPOCH FROM t_end - t_start)::numeric, 2), row_count;


	  -- Final Logging
  global_end := clock_timestamp();
  RAISE NOTICE '======== SUCCESS: Silver layer loaded at % ========', global_end;
  RAISE NOTICE 'Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM global_end - global_start)::numeric, 2);

EXCEPTION
  WHEN OTHERS THEN
    RAISE NOTICE '========== ERROR OCCURRED DURING LOADING BRONZE LAYER =========='; 
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'SQLSTATE Code: %', SQLSTATE;
  
END;
$$;

