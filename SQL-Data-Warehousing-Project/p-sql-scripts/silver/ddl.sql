CREATE TABLE silver.crm_cust_info(
cst_id INTEGER,
cst_key	VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.crm_prd_info(
prd_id INTEGER,
cat_id VARCHAR(50),
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INTEGER,
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
SELECT*FROM silver.crm_prd_info

CREATE TABLE silver.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INTEGER,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INTEGER,
sls_quantity INTEGER,
sls_price INTEGER,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE silver.erp_PX_CAT_G1V2(
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50),
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE silver.erp_LOC_A101(
CID varchar(50),
CNTRY varchar(50),
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE silver.erp_CUST_AZ12(
CID VARCHAR(50),
BDATE DATE,
GEN VARCHAR(50),
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


SELECT * FROM silver.crm_cust_info



