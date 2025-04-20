-- SCRIPT--------------
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
-- ------------------------------
-- ----FINAL CHECK---------
SELECT*
FROM silver.erp_CUST_AZ12;


-- CHECK FOT INVALID GEN 

select distinct gen
from bronze.erp_cust_az12

