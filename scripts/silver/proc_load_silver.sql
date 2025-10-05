/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @start_time DATETIME, 
        @end_time DATETIME, 
        @batch_start_time DATETIME, 
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        -------------------------
        -- silver.crm_cust_info
        -------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Data Into: silver.crm_cust_info';

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
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_cust_info): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -------------------------
        -- silver.crm_prd_info (fixed)
        -------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting Data Into: silver.crm_prd_info';

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
            t.prd_id,
            t.cat_id,
            t.prd_key_short AS prd_key,
            t.prd_nm,
            t.prd_cost,
            CASE 
                WHEN UPPER(t.prd_line_code) = 'M' THEN 'Mountain'
                WHEN UPPER(t.prd_line_code) = 'R' THEN 'Road'
                WHEN UPPER(t.prd_line_code) = 'S' THEN 'Other Sales'
                WHEN UPPER(t.prd_line_code) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            t.prd_start_dt,
            CAST(
                DATEADD(day, -1,
                    LEAD(t.prd_start_dt) OVER (PARTITION BY t.prd_key_short ORDER BY t.prd_start_dt)
                ) AS DATE
            ) AS prd_end_dt
        FROM (
            SELECT
                prd_id,
                REPLACE(SUBSTRING(ISNULL(prd_key,''), 1, 5), '-', '_') AS cat_id,
                CASE WHEN LEN(ISNULL(prd_key,'')) > 6 THEN RIGHT(prd_key, LEN(prd_key) - 6) ELSE prd_key END AS prd_key_short,
                prd_nm,
                ISNULL(prd_cost, 0) AS prd_cost,
                LTRIM(RTRIM(ISNULL(prd_line,''))) AS prd_line_code,
                CAST(prd_start_dt AS DATE) AS prd_start_dt
            FROM bronze.crm_prd_info
        ) AS t;

        PRINT '>> Rows Inserted into silver.crm_prd_info: ' + CAST(@@ROWCOUNT AS NVARCHAR(12));

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_prd_info): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -------------------------
        -- silver.crm_sales_details
        -------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserting Data Into: silver.crm_sales_details';

        INSERT INTO silver.crm_sales_details (
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
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_sales_details): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -------------------------
        -- silver.erp_cust_az12
        -------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserting Data Into: silver.erp_cust_az12';

        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid, 
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
            CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                 ELSE 'n/a' END AS gen
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_cust_az12): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        -------------------------
        -- silver.erp_loc_a101
        -------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Inserting Data Into: silver.erp_loc_a101';

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', '') AS cid, 
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_loc_a101): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -------------------------
        -- silver.erp_px_cat_g1v2 (DYNAMIC: resilient to missing columns)
        -------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2 (dynamic, schema-resilient)';

        DECLARE 
            @sql NVARCHAR(MAX),
            @select_list NVARCHAR(MAX),
            @col NVARCHAR(128);

        -- Build select list dynamically: for each expected column, check if it exists in source; if not, select NULL AS <col>
        SET @select_list = '';

        -- helper to append column or NULL AS column
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'bronze.erp_px_cat_g1v2') AND name = 'id')
            SET @select_list = @select_list + 'id';
        ELSE
            SET @select_list = @select_list + 'NULL AS id';

        SET @select_list = @select_list + ', ';

        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'bronze.erp_px_cat_g1v2') AND name = 'cat')
            SET @select_list = @select_list + 'cat';
        ELSE
            SET @select_list = @select_list + 'NULL AS cat';

        SET @select_list = @select_list + ', ';

        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'bronze.erp_px_cat_g1v2') AND name = 'subcat')
            SET @select_list = @select_list + 'subcat';
        ELSE
            SET @select_list = @select_list + 'NULL AS subcat';

        SET @select_list = @select_list + ', ';

        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'bronze.erp_px_cat_g1v2') AND name = 'maintenance')
            SET @select_list = @select_list + 'maintenance';
        ELSE
            SET @select_list = @select_list + 'NULL AS maintenance';

        -- Build and execute a dynamic insert
        SET @sql = N'
            INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
            SELECT ' + @select_list + '
            FROM bronze.erp_px_cat_g1v2;
        ';

        EXEC sp_executesql @sql;

        PRINT '>> Rows Inserted into silver.erp_px_cat_g1v2: ' + CAST(@@ROWCOUNT AS NVARCHAR(12));

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_px_cat_g1v2): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -------------------------
        -- Finish
        -------------------------
        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';

    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
        -- rethrow if you prefer: THROW;
    END CATCH
END;
