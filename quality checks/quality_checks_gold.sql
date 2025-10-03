--check if we have dublicates in primary key
select cst_id,COUNT(*)
from
(select
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid)t
group by cst_id
having count(*) >1


--handle gendre when the master data is crm "cst_gendr"
select distinct
ci.cst_gndr,
ca.gen,
case when ci.cst_gndr != 'N/A' then ci.cst_gndr
else coalesce(ca.gen,'N/A')
end new_gndr
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
order by 1,2


