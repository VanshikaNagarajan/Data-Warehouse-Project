CALL silver.load_silver();

CREATE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN

-- 1
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
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
	CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_num
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE row_num = 1;

-- 2
TRUNCATE TABLE silver.crm_prd_info;

WITH prd_data AS (
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS prd_end_dt
    FROM bronze.crm_prd_info
)
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
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    CAST(prd_end_dt AS DATE)  
FROM prd_data;

-- 3
TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details(
sls_ord_num,
sls_prd_key ,
sls_cust_id ,
sls_order_dt ,
sls_ship_dt ,
sls_due_dt ,
sls_sales ,
sls_quantity ,
sls_price 
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    
    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
    END AS sls_order_dt,
   
	CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
    END AS sls_ship_dt,
  
	CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
    END AS sls_due_dt,
	
     CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
	
    CASE 
        WHEN sls_price IS NULL OR sls_price < 0 
        THEN sls_sales / NULLIF(sls_quantity, 0)  -- Avoid division by zero
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

-- 4
TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)
SELECT
    
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid) - 3)
        ELSE cid
    END AS cid,
    CASE 
	WHEN bdate>current_date then null
	else bdate
	end as bdate,
    
	CASE WHEN UPPER(TRIM(gen)) in ('F', 'FEMALE') then 'Female'
	 WHEN UPPER(TRIM(gen)) in ('M', 'MALE') then 'Male'
	else 'n/a'
	end as gen
FROM bronze.erp_CUST_AZ12;

-- 5
TRUNCATE TABLE silver.erp_loc_a101;

insert into silver.erp_loc_a101(cid,cntry)
SELECT
REPLACE(CID,'-','')cid,
CASE WHEN TRIM(cntry) = 'DE' then 'Germany'
	WHEN TRIM(cntry) = 'US' then 'United States'
	WHEN TRIM(cntry) = '' or cntry is null then 'n/a'
	else trim(cntry)
	end as cntry
FROM bronze.erp_LOC_A101;

-- 6
TRUNCATE silver.erp_px_cat_g1v2;
insert into silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance)
select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;
END;
$$;
