/*
======================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
======================================================
Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the `BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;
======================================================
*/
CREATE OR ALTER  PROCEDURE [bronze].[load_bronze] AS
BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME,@total_time DATETIME
	BEGIN TRY   
		PRINT'==============================='
		PRINT'     BRONZE LAYER LOADING      '
		PRINT'==============================='
		PRINT''
		PRINT'-------------------------------'
		PRINT'TRUNCATING bronze.crm_cust_info'
		PRINT'-------------------------------'
		SET @start_time =GETDATE()
		TRUNCATE TABLE bronze.crm_cust_info   --FULL LOAD,TRUNCATE & LOAD Source File to prevent Dupliaction
		PRINT'-------------------------------'
		PRINT'LOADING bronze.crm_cust_info'
		PRINT'-------------------------------'
		BULK INSERT bronze.crm_cust_info
		FROM'D:\Ayyan SQL Course All Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		PRINT'-------------------------------'
		PRINT'TRUNCATING bronze.crm_prd_info'
		PRINT'-------------------------------'
		TRUNCATE TABLE bronze.crm_prd_info   --FULL LOAD,TRUNCATE & LOAD Source File to prevent Dupliaction
		PRINT'LOADING bronze.crm_cust_info'
		PRINT'-------------------------------'
		BULK INSERT bronze.crm_prd_info
		FROM'D:\Ayyan SQL Course All Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK,
			KEEPNULLS
		); 

		PRINT'-----------------------------------'
		PRINT'TRUNCATING bronze.crm_sales_details'
		PRINT'-----------------------------------'
		TRUNCATE TABLE bronze.crm_sales_details   --FULL LOAD,TRUNCATE & LOAD Source File to prevent Dupliaction
		PRINT'-----------------------------------'
		PRINT'LOADING bronze.crm_sales_details'
		PRINT'-----------------------------------'
		BULK INSERT bronze.crm_sales_details
		FROM'D:\Ayyan SQL Course All Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);	

		PRINT'-----------------------------------'
		PRINT'TRUNCATING bronze.erp_cust_az12'
		PRINT'-----------------------------------'
		TRUNCATE TABLE bronze.erp_cust_az12   --FULL LOAD,TRUNCATE & LOAD Source File to prevent Dupliaction
		PRINT'-----------------------------------'
		PRINT'LOADING bronze.crm_sales_details'
		PRINT'-----------------------------------'
		BULK INSERT bronze.erp_cust_az12
		FROM'D:\Ayyan SQL Course All Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		PRINT'-----------------------------------'
		PRINT'TRUNCATING bronze.erp_loc_a101'
		PRINT'-----------------------------------'
		TRUNCATE TABLE bronze.erp_loc_a101   --FULL LOAD,TRUNCATE & LOAD Source File to prevent Dupliaction
		PRINT'-----------------------------------'
		PRINT'LOADING bronze.crm_sales_details'
		PRINT'-----------------------------------'
		BULK INSERT bronze.erp_loc_a101
		FROM'D:\Ayyan SQL Course All Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		PRINT'-----------------------------------'
		PRINT'TRUNCATING bronze.erp_px_cat_g1v2'
		PRINT'-----------------------------------'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2   --FULL LOAD,TRUNCATE & LOAD Source File to prevent Dupliaction
		PRINT'-----------------------------------'
		PRINT'LOADING bronze.erp_px_cat_g1v2'
		PRINT'-----------------------------------'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM'D:\Ayyan SQL Course All Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time =GETDATE()
		PRINT''
		PRINT'Total Time During FULL LOAD: '+ CAST(DATEDIFF(microsecond,@start_time,@end_time) AS NVARCHAR)+ ' micro seconds'
		END TRY
		BEGIN CATCH
			PRINT'ERROR MESSAGE'+ERROR_MESSAGE();
			PRINT('ERROR LINE'+CAST(ERROR_LINE() AS NVARCHAR));
		END CATCH

END
