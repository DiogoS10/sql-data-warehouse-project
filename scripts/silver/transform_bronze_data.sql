-- Loading silver.crm_cust_info

SELECT 
	cst_id,
    TRIM(cst_key) AS cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
		WHEN cst_marital_status = 'M' THEN 'Married'
        WHEN cst_marital_status = 'S' THEN 'Single'
        ELSE 'n/a'
    END cst_marital_status, -- Normalize marital status values to readable format
    CASE
		WHEN cst_gndr = 'M' THEN 'Male'
        WHEN cst_gndr = 'F' THEN 'Female'
        ELSE 'n/a'
	END cst_gndr, -- Normalize gender values to readable format
    cst_create_date
FROM( 
	SELECT 
	*,
    ROW_NUMBER() OVER(PARTITION BY cst_key ORDER BY cst_create_date DESC) as recent
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL) t
WHERE recent = 1;  -- Select the most recent record per custome

-- Loading silver.crm_prd_info

SELECT 
	prd_id,
    REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_id, -- Extract category ID
    SUBSTRING(TRIM(prd_key), 7, LENGTH(prd_key)) AS prd_key, -- Extract product key
    TRIM(prd_nm) AS prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    CASE 
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a' -- Map product line codes to descriptive values
    END cst_marital_status,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    DATE_SUB(CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE), INTERVAL 1 DAY) as prd_end_dt -- Calculate end date as one day before the next start date
FROM bronze.crm_prd_info;




