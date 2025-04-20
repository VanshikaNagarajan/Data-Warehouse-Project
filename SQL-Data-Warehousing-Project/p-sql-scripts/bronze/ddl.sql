CREATE TABLE bronze.crm_cust_info(
cst_id INTEGER,
cst_key	VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
)
SELECT * FROM bronze.crm_cust_info

CREATE TABLE bronze.crm_prd_info(
prd_id INTEGER,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INTEGER,
prd_line VARCHAR(50),
prd_start_dt TIMESTAMP,
prd_end_dt TIMESTAMP
);

SELECT * FROM bronze.crm_prd_info

CREATE TABLE bronze.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INTEGER,
sls_order_dt INTEGER,
sls_ship_dt INTEGER,
sls_due_dt INTEGER,
sls_sales INTEGER,
sls_quantity INTEGER,
sls_price INTEGER
);

SELECT * FROM bronze.crm_sales_details


CREATE TABLE bronze.erp_PX_CAT_G1V2(
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50)
);

SELECT * FROM bronze.erp_PX_CAT_G1V2


CREATE TABLE bronze.erp_LOC_A101(
CID varchar(50),
CNTRY varchar(50)
);
SELECT * FROM bronze.erp_LOC_A101


CREATE TABLE bronze.erp_CUST_AZ12(
CID VARCHAR(50),
BDATE DATE,
GEN VARCHAR(50)
);
SELECT * FROM bronze.erp_CUST_AZ12


