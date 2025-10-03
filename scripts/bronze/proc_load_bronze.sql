CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME; 
	BEGIN TRY
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\Data engineering Projects\datawarehouse project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		Print '>> Load Duration: '+CAST(DATEDIFF(Second,@start_time,@end_time) AS VARCHAR) +' seconds';
		Print '>> -------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\Data engineering Projects\datawarehouse project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		Print '>> Load Duration: '+CAST(DATEDIFF(Second,@start_time,@end_time) AS VARCHAR) +' seconds';
		Print '>> -------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\Data engineering Projects\datawarehouse project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		Print '>> Load Duration: '+CAST(DATEDIFF(Second,@start_time,@end_time) AS VARCHAR) +' seconds';
		Print '>> -------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\Data engineering Projects\datawarehouse project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		Print '>> Load Duration: '+CAST(DATEDIFF(Second,@start_time,@end_time) AS VARCHAR) +' seconds';
		Print '>> -------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\Data engineering Projects\datawarehouse project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		Print '>> Load Duration: '+CAST(DATEDIFF(Second,@start_time,@end_time) AS VARCHAR) +' seconds';
		Print '>> -------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\Data engineering Projects\datawarehouse project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		Print '>> Load Duration: '+CAST(DATEDIFF(Second,@start_time,@end_time) AS VARCHAR) +' seconds';
		Print '>> -------------------------------';
	END TRY
	BEGIN CATCH
		PRINT '================================================'
		PRINT 'Error Occured'
		PRINT 'Error Message '+ERROR_MESSAGE();
		PRINT 'Error Message '+CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message '+CAST(ERROR_STATE() AS VARCHAR);
		PRINT '================================================'
	END CATCH
END

EXEC bronze.load_bronze;