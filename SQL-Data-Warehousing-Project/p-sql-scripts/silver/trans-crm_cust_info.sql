-- Data Transformation:

-- 1. to check if there exist any multiple ids for PK
SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id is NULL

-- to rank the rows of the duplicate ids to get the newest
SELECT *, 
       ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_num
FROM bronze.crm_cust_info

-- to only get the unique id and drop the duplicates
SELECT *
FROM(
SELECT
*,
  ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_num
FROM bronze.crm_cust_info
)
WHERE row_num = 1



-- 2. to check the unwanted spaces for firstname
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


-- to check the unwanted spaces for lastname
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


-- to check the unwanted spaces for gender
-- RESULT: Better quality, no spaces
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_gndr)




-- final check for cleaned table
-- RESULT: empty
SELECT cst_id, COUNT(*) 
FROM bronze.crm_cust_info 
GROUP BY cst_id 
HAVING COUNT(*) > 1;



-- FINAL SCRIPT to load transformed data into silver
-- --------------------------------
-- --------------------------------
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
-- ----------------------------------------------------------------------------------------

-- final check for transformed data loaded into silver
SELECT * FROM silver.crm_cust_info
