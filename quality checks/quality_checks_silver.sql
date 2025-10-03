--test here

select 
prd_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from
bronze.crm_prd_info

--check null or dublicated items in prd_id
	select prd_id,
	count(*)
	from bronze.crm_prd_info
	group by prd_id
	having count(*)>1 or prd_id is null

--check start date and end date
select
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt



select
prd_key,
prd_start_dt,
prd_end_dt,
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt_test
from bronze.crm_prd_info

--check buisness rules
--sales = quantity * price
select distinct
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
	then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales,

case when sls_price is null or sls_price <=0
	then sls_sales / nullif(sls_quantity,0)
	else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
order by sls_sales,sls_quantity,sls_price

--check dates ranges in erp_cust_az12
select distinct
bdate
from 
bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE()

--data gendre standardlizations  in erp_cust_az12
select
case when trim(upper(gen)) in ('F','FEMALE') then 'Female'
	 when trim(upper(gen)) in ('M','MALE') then 'Male'
	 else 'n/a'
end as gen
from bronze.erp_cust_az12