-- Loading silver.crm_cust_info

TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    TRIM(cst_key) AS cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN cst_marital_status = 'M' THEN 'Married'
        WHEN cst_marital_status = 'S' THEN 'Single'
        ELSE 'n/a'
    END AS cst_marital_status, -- Normalize marital status values to readable format
    CASE
        WHEN cst_gndr = 'M' THEN 'Male'
        WHEN cst_gndr = 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gndr, -- Normalize gender values to readable format
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY TRIM(cst_key)
               ORDER BY cst_create_date DESC
           ) AS recent
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE recent = 1; -- Select the most recent record per customer

-- Loading silver.crm_prd_info

TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_id,
    SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key))) AS prd_key,
    TRIM(prd_nm) AS prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    DATE_SUB(
        CAST(
            LEAD(CAST(prd_start_dt AS DATE)) OVER (
                PARTITION BY SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key)))
                ORDER BY CAST(prd_start_dt AS DATE)
            ) AS DATE
        ),
        INTERVAL 1 DAY
    ) AS prd_end_dt
FROM bronze.crm_prd_info;

-- Loading silver.crm_sales_details

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
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
				WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
				ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
				ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
				ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;

-- Loading silver.erp_cust_az12

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (
			cid,
            bdate,
            gen
		)
		SELECT
			CASE 
				WHEN cid LIKE 'NAS%' THEN substring(TRIM(cid), 4, length(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END cid,
			CASE
				WHEN bdate > NOW() THEN NULL
				ELSE bdate
			END bdate, -- Set future birthdates to NULL
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;

-- Loading silver.erp_loc_a101

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (
			cid,
            cntry
		)
		SELECT 
			REPLACE(cid, '-', '') as cid,
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;

-- Loading silver.erp_px_cat_g1v2

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2 (
			id,
            cat,
            subcat,
            maintenance
		)
		SELECT 
			id, 
			cat, 
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;




