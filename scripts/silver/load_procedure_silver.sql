/*
create silver tables same as bronze tables
creates procedure for loading the data from bronze layer to silver layer
  > truncate the data in table before loading
  > load the data into tables
  > shows the execution time for each data load and whole batch
  > error handling manages the errors
  
use:
  EXEC silver.load_silver
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME;
    
    BEGIN TRY
        
        PRINT '==============================================='
        PRINT 'DATA LOAD FROM BRONZE TO SILVER'
        PRINT '==============================================='
    
        -- ===============================================
        -- DATA FROM SOURCE CRM
        -- ===============================================
        PRINT 'DATA FROM BRONZE CRM'
        SET @batch_start_time = GETDATE()
        -- Table 1: crm_cust_info
        SET @start_time = GETDATE()
        PRINT 'TRUNCATE TABLE silver.crm_cust_info'
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT 'LOAD DATA INTO silver.crm_cust_info'
        insert into 
        silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gender,
        cst_create_date)
        select 
	    cst_id,
	    cst_key,
	    trim(cst_firstname) as cst_firstname,
	    trim(cst_lastname) as cst_lastname,
	    CASE 
	    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'married'
	    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'single'
	    ELSE 'NA'
	    END AS cst_marital_status,
	    CASE 
	    WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'male'
	    WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'female'
	    ELSE 'NA' 
	    END AS cst_gender,
	    cst_create_date
        from(
        select 
	        *,
	        row_number() over(partition by cst_id order by cst_create_date DESC) as latest
        from
        bronze.crm_cust_info) t where latest = 1 and cst_id is not null;

        SET @end_time = GETDATE()
        PRINT '>> load duration : ' + cast(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------'

        -- Table 2: crm_prd_info
        SET @start_time = GETDATE()
        PRINT 'TRUNCATE TABLE silver.crm_prd_info'
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT 'LOAD DATA INTO silver.crm_prd_info'
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
select 
	prd_id,
	replace(substring(prd_key,1,5),'-','_') as cat_id,
	substring(prd_key,7,len(prd_key)) as prd_key,
	prd_nm,
	isnull(prd_cost,0) as prd_cost,
	case when upper(trim(prd_line)) = 'R' THEN 'Road'
		 when upper(trim(prd_line)) = 'M' THEN 'Mountain'
		 when upper(trim(prd_line)) = 'S' THEN 'other sales'
		 when upper(trim(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'NA' END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(DATEADD(day, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) 
    AS DATE) AS prd_end_dt
	from bronze.crm_prd_info;
        SET @end_time = GETDATE()
        PRINT '>> load duration : ' + cast(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------'

        -- Table 3: crm_sales_details
        SET @start_time = GETDATE()
        PRINT 'TRUNCATE TABLE silver.crm_sales_details'
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT 'LOAD DATA INTO silver.crm_sales_details'
        INSERT INTO silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt is null or len(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt is null or len(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt is null or len(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
		END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price is null or sls_sales <= 0
	THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
	END AS sls_price
from bronze.crm_sales_details;
        SET @end_time = GETDATE()
        PRINT '>> load duration : ' + cast(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------'

        -- ===============================================
        -- DATA FROM SOURCE ERP
        -- ===============================================
        PRINT 'DATA FROM BRONZE ERP'

        -- Table 4: erp_cust_az12
        SET @start_time = GETDATE()
        PRINT 'TRUNCATE TABLE silver.erp_cust_az12'
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT 'LOAD DATA INTO silver.erp_cust_az12'
        
INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen)
select
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid , 4 , LEN(cid))
	ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
	END AS bdate,
	CASE WHEN trim(upper(gen)) IN ('M', 'MALE') THEN 'Male'
		 WHEN trim(upper(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 ELSE 'NA'
	END AS gen
from bronze.erp_cust_az12;
        SET @end_time = GETDATE()
        PRINT '>> load duration : ' + cast(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------'

        -- Table 5: erp_loc_a101
        SET @start_time = GETDATE()
        PRINT 'TRUNCATE TABLE silver.erp_loc_a101'
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT 'LOAD DATA INTO silver.erp_loc_a101'
        insert into silver.erp_loc_a101(
cid,
cntey
)
select 
	replace(cid , '-' , '') AS cid,
	CASE
	WHEN trim(cntey) = 'DE' THEN 'Germany'
	WHEN TRIM(cntey) in ('US','USA') THEN 'United States'
	WHEN TRIM(cntey) = '' or cntey is null THEN 'NA'
	ELSE TRIM(cntey)
	END AS cntry
	from bronze.erp_loc_a101;
        SET @end_time = GETDATE()
        PRINT '>> load duration : ' + cast(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------'

        -- Table 6: erp_px_cat_g1v2
        SET @start_time = GETDATE()
        PRINT 'TRUNCATE TABLE silver.erp_px_cat_g1v2'
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT 'LOAD DATA INTO silver.erp_px_cat_g1v2'
        INSERT INTO silver.erp_px_cat_g1v2(
id,
cat,
subcat,
MAINTENANCE)
select 
id,
cat,
subcat,
MAINTENANCE
FROM bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE()
        PRINT '>> load duration : ' + cast(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------'
        
        SET @batch_end_time = GETDATE()
    PRINT 'load duration for whole data:  '+ CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) +
' seconds'
    END TRY
    
    BEGIN CATCH
        PRINT '==============================================='
        PRINT 'ERROR OCCURED DURING LOADING THE DATA';
        PRINT 'ERROR MESSAGE : '+ ERROR_MESSAGE();
        PRINT '==============================================='
    END CATCH
    
END;
EXEC silver.load_silver;

