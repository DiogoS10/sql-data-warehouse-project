SELECT 
	cst_id,
    TRIM(cst_key) AS cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
		WHEN cst_marital_status = 'M' THEN 'Married'
        WHEN cst_marital_status = 'S' THEN 'Single'
        ELSE 'n/a'
    END cst_marital_status,
    CASE
		WHEN cst_gndr = 'M' THEN 'Male'
        WHEN cst_gndr = 'F' THEN 'Female'
        ELSE 'n/a'
	END cst_gndr,
    cst_create_date
FROM( 
	SELECT 
	*,
    ROW_NUMBER() OVER(PARTITION BY cst_key ORDER BY cst_create_date DESC) as recent
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL) t
WHERE recent = 1; 

