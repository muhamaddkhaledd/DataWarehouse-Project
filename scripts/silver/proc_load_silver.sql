CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	declare @start_time datetime,@end_time datetime,@batch_start_time datetime,@batch_end_time datetime
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		print '============================='
		print 'loading silver layers'
		print '============================='

		print '============================='
		print 'loading crm tables'
		print '============================='
		--silver.crm_cust_info
		SET @start_time = GETDATE();
		print '>> Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		print '>> Inserting Data Into: silver.crm_cust_info';
		insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case when cst_marital_status = upper(trim('S')) then 'Single'
			 when cst_marital_status = upper(trim('M')) then 'Married'
			 else 'N/A'
		end cst_marital_status

		,
		case when cst_gndr = upper(trim('F')) then 'Female'
			 when cst_gndr = upper(trim('M')) then 'Male'
			 else 'N/A'
		end cst_gndr
		,
		cst_create_date
		from (
		select
		*,
		ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
		 from bronze.crm_cust_info
		 where cst_id is not null
		)t where flag_last = 1
		SET @end_time = GETDATE();
		print '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as varchar) +' seconds';



		--silver.crm_prd_info
		set @start_time = GETDATE();
		print '>> Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		print '>> Inserting Data Into: silver.crm_prd_info';
		insert into silver.crm_prd_info(
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
		Replace(SUBSTRING(prd_key,1,5),'-','_')as cat_id, --Extract category ID
		SUBSTRING(prd_key,7,len(prd_key)) as prd_key,	  --Extract product key
		prd_nm,
		isnull(prd_cost,0 )as prd_cost,
		case when UPPER(trim(prd_line)) = 'M' then 'Mountain'
			 when UPPER(trim(prd_line)) = 'R' then 'Road'
			 when UPPER(trim(prd_line)) = 'S' then 'Other sales'
			 when UPPER(trim(prd_line)) = 'T' then 'Touring'
		else 'N/A'
		end as prd_line, --Map product line codes to descriptive values
		prd_start_dt,
		dateadd(day,-1,lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as prd_end_dt --calculate end date
		from bronze.crm_prd_info
		SET @end_time = GETDATE();
		print '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as varchar) +' seconds';



		--silver.crm_sales_details
		set @start_time = GETDATE();
		print '>> Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		print '>> Inserting Data Into: silver.crm_sales_details';
		insert into silver.crm_sales_details
		(
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
		case when sls_order_dt = 0 or len(sls_order_dt) !=8 then null
			else cast(cast(sls_order_dt as varchar) as DATE)
		end as sls_order_dt,

		case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null
			else cast(cast(sls_ship_dt as varchar) as DATE)
		end as sls_ship_dt,

		case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null
			else cast(cast(sls_due_dt as varchar) as DATE)
		end as sls_due_dt,
		case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
			then sls_quantity * abs(sls_price)
			else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0
			then sls_sales / nullif(sls_quantity,0)
			else sls_price
		end as sls_price
		from bronze.crm_sales_details
		SET @end_time = GETDATE();
		print '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as varchar) +' seconds';




		--silver.erp_cust_az12
		set @start_time = GETDATE();
		print '>> Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		print '>> Inserting Data Into: silver.erp_cust_az12';
		insert into silver.erp_cust_az12(
		cid ,
		bdate,
		gen
		)
		select
		case when cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
		else cid
		end as cid,
		case when  bdate > GETDATE() then null
		else bdate
		end as bdate,
		case when trim(upper(gen)) in ('F','FEMALE') then 'Female'
			 when trim(upper(gen)) in ('M','MALE') then 'Male'
			 else 'N/A'
		end as gen
		from bronze.erp_cust_az12
		SET @end_time = GETDATE();
		print '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as varchar) +' seconds';




		--silver.erp_loc_a101
		set @start_time = GETDATE()
		print '>> Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		print '>> Inserting Data Into: silver.erp_loc_a101';
		insert into silver.erp_loc_a101(
		cid,cntry
		)
		select
		REPLACE(cid,'-','') as cid,
		case when trim(cntry) = 'DE' then 'Germany'
			 when trim(cntry) in ('US','USA') then 'United States'
			 when trim(cntry) ='' or cntry is null then 'N/A'
			 else trim(cntry)
		end cntry
		from bronze.erp_loc_a101
		SET @end_time = GETDATE();
		print '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as varchar) +' seconds';




		--silver.erp_px_cat_g1v2
		set @start_time = GETDATE()
		print '>> Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2
		(id,cat,subcat,maintenance)
		select
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		print '>> Load Duration: '+ cast(DATEDIFF(second,@start_time,@end_time) as varchar) +' seconds';

		SET @batch_end_time = GETDATE()
		print 'loading silver layer is completed'
		print 'Total load duration: '+ cast(datediff(second,@batch_start_time,@batch_end_time) as varchar)+' seconds'
	END TRY
	BEGIN CATCH
	print '========================='
	print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	print 'Error Message'+ Error_Message();
	print 'Error Message'+ cast(Error_Number() as varchar);
	print 'Error Message'+ cast(Error_State() as varchar);
	print '========================='
	END CATCH
END