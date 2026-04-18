/*
creates procedure for loading the data into tables in bronze layer
  > truncate the data in table before loading
  > load the data into tables
  > shows the execution time for each data load and whole batch
  > error handling manages the errors
  
use:
  EXEC bronze.load_bronze
*/





CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME;
    
    BEGIN TRY
        
        PRINT '==============================================='
        PRINT 'DATA LOAD FROM SOURCE TO TABLES'
        PRINT '==============================================='
    
        -- ===============================================
        -- DATA FROM SOURCE CRM
        -- ===============================================
        PRINT 'DATA FROM SOURCE CRM'
        SET @batch_start_time = GETDATE()
        -- Table 1: crm_cust_info
        SET @start_time = GETDATE()
        PRINT 'TRUNCATE TABLE bronze.crm_cust_info'
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT 'LOAD DATA INTO bronze.crm_cust_info'
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\thand\OneDrive\Desktop\DW project\source_crm\cust_info.csv'
        WITH(FIRSTROW = 2, FIELDTERMINATOR = ',');
        SET @end_time = GETDATE()
        PRINT '>> load duration : ' + cast(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------'

        -- Table 2: crm_prd_info
        SET @start_time = GETDATE()
        PRINT 'TRUNCATE TABLE bronze.crm_prd_info'
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT 'LOAD DATA INTO bronze.crm_prd_info'
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\thand\OneDrive\Desktop\DW project\source_crm\prd_info.csv'
        WITH(FIRSTROW = 2, FIELDTERMINATOR = ',');
        SET @end_time = GETDATE()
        PRINT '>> load duration : ' + cast(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------'

        -- Table 3: crm_sales_details
        SET @start_time = GETDATE()
        PRINT 'TRUNCATE TABLE bronze.crm_sales_details'
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT 'LOAD DATA INTO bronze.crm_sales_details'
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\thand\OneDrive\Desktop\DW project\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',');
        SET @end_time = GETDATE()
        PRINT '>> load duration : ' + cast(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------'

        -- ===============================================
        -- DATA FROM SOURCE ERP
        -- ===============================================
        PRINT 'DATA FROM SOURCE ERP'

        -- Table 4: erp_cust_az12
        SET @start_time = GETDATE()
        PRINT 'TRUNCATE TABLE bronze.erp_cust_az12'
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT 'LOAD DATA INTO bronze.erp_cust_az12'
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\thand\OneDrive\Desktop\DW project\source_erp\CUST_AZ12.csv'
        WITH(FIRSTROW = 2, FIELDTERMINATOR = ',');
        SET @end_time = GETDATE()
        PRINT '>> load duration : ' + cast(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------'

        -- Table 5: erp_loc_a101
        SET @start_time = GETDATE()
        PRINT 'TRUNCATE TABLE bronze.erp_loc_a101'
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT 'LOAD DATA INTO bronze.erp_loc_a101'
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\thand\OneDrive\Desktop\DW project\source_erp\LOC_A101.csv'
        WITH(FIRSTROW = 2, FIELDTERMINATOR = ',');
        SET @end_time = GETDATE()
        PRINT '>> load duration : ' + cast(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------'

        -- Table 6: erp_px_cat_g1v2
        SET @start_time = GETDATE()
        PRINT 'TRUNCATE TABLE bronze.erp_px_cat_g1v2'
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT 'LOAD DATA INTO bronze.erp_px_cat_g1v2'
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\thand\OneDrive\Desktop\DW project\source_erp\PX_CAT_G1V2.csv'
        WITH(FIRSTROW = 2, FIELDTERMINATOR = ',');
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
EXEC bronze.load_bronze;

