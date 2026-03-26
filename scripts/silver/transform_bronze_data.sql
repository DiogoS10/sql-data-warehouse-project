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




