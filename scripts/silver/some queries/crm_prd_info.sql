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
