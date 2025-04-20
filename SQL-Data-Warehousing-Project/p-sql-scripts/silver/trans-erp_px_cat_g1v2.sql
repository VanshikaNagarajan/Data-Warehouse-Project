select * from bronze.erp_px_cat_g1v2
-- good quality data no changes
-- script----------------------------------
TRUNCATE TABLE silver.erp_px_cat_g1v2;

insert into silver.erp_px_cat_g1v2(id,cat,
subcat,
maintenance)
select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2
-- ------------------------
--FINAL CHECK-----------------------------------------

select*
from silver.erp_px_cat_g1v2