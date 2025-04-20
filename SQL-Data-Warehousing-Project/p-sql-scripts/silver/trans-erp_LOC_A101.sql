-- script---------------------------------------
TRUNCATE TABLE silver.erp_LOC_A101;

insert into silver.erp_LOC_A101(cid,cntry)
SELECT
REPLACE(CID,'-','')cid,
CASE WHEN TRIM(cntry) = 'DE' then 'Germany'
	WHEN TRIM(cntry) = 'US' then 'United States'
	WHEN TRIM(cntry) = '' or cntry is null then 'n/a'
	else trim(cntry)
	end as cntry
FROM bronze.erp_LOC_A101 
-- ----------------------------------------
-- FINAL CHECK--------------------------------------------- 
select*
FROM silver.erp_loc_a101

