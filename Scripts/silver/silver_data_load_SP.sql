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
	  This stored procedure does not accept any parameters.
    Return a string values with sucess msg.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


create or replace procedure data_warehousing.silver.load_CRM_bronze_to_silver()
returns string
language sql
as 
begin 
    ---- loading crm_cust_info
    truncate table data_warehousing.silver.crm_cust_info;
    
    insert into data_warehousing.silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
    select 
        cst_id,
        cst_key,
        trim(cst_firstname) as cst_firstname,
        trim(cst_lastname) as cst_Lastname,
        case 
            when upper(cst_marital_status) = 'M' then 'Married'
            when upper(cst_marital_status) = 'S' then 'Single'
            else 'N/A'
        end as cst_marital_status,
        case 
            when upper(cst_gndr) = 'M' then 'Male'
            when upper(cst_gndr) = 'F' then 'Female'
            else 'N/A'
        end as cst_gndr,
        cst_create_date
        from data_warehousing.bronze.crm_cust_info;
    
    ----- loading crm_prod_info
    truncate table data_warehousing.silver.crm_prd_info;
    
    insert into data_warehousing.silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        select  prd_id,
            replace(substr(prd_key, 1, 5), '-', '_') as cat_id,
            substr(prd_key, 7) as prd_key,
            prd_nm,
            ifnull(prd_cost, 0) as prd_cost,
            case upper(trim(prd_line))
                when 'R' then 'Road'
                when 'S' then 'Other Sales'
                when 'M' then 'Mountain'
                when 'T' then 'Touring'
                else 'N/A'
            end as prd_line,
            to_date(prd_start_dt) as prd_start_dt,
            to_date(lead(prd_start_dt) over (partition by prd_key order by prd_id))-1 as prd_end_dt
        from data_warehousing.bronze.crm_prd_info order by prd_id;
    
    ---- loading crm_sales_details
    truncate table data_warehousing.silver.crm_sales_details;
    
    insert into data_warehousing.silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    select sls_ord_num, sls_prd_key, sls_cust_id,
        case when sls_order_dt = 0 or len(sls_order_dt) <8 then null
            else to_date(sls_order_dt::varchar, 'YYYYMMDD') 
        end as sls_order_dt,
        case when sls_ship_dt = 0 or len(sls_ship_dt) <8 then null
            else to_date(sls_ship_dt::varchar, 'YYYYMMDD') 
        end as sls_ship_dt,
        case when sls_due_dt = 0 or len(sls_due_dt) <8 then null
            else to_date(sls_due_dt::varchar, 'YYYYMMDD') 
        end as sls_due_dt,
        case when sls_sales <= 0 or sls_sales is null 
            then abs(sls_price * sls_quantity)
            else sls_sales
        end as sls_sales,
        sls_quantity,
        case when sls_price <= 0 or sls_price is null
            then (abs(sls_sales)/abs(sls_quantity))::int
            else sls_price::int
        end as sls_price
    from data_warehousing.bronze.crm_sales_details;
    
    ----------------
    return 'All CRM loaded from bronze to silver';
end;

create or replace procedure data_warehousing.silver.load_ERP_bronze_to_silver()
returns string
language sql
as
begin
    ---- loading erp_cust_az12
    truncate table data_warehousing.silver.erp_cust_az12;
    
    insert into data_warehousing.silver.erp_cust_az12 (cid, bdate, gen)
    select 
        case when cid like 'NAS%' then substring(cid, 4)
            else cid
        end as cid,
        case when bdate > current_date() then null
            else bdate
        end as bdate,
        case when upper(trim(gen)) in ('M','MALE') then 'Male'
            when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
            else 'N/A'
        end as gen
    from data_warehousing.bronze.erp_cust_az12;
    
    ---- loading erp_loc_a101
    truncate table data_warehousing.silver.erp_loc_a101;
    
    insert into data_warehousing.silver.erp_loc_a101 (cid, cntry)
    select replace(cid, '-', '') as cid,
        case 
            when trim(cntry) = 'DE' then 'Germany'
            when trim(cntry) in ('US','USA') then 'United States'
            when trim(cntry) = '' or cntry is null then 'N/A'
            else trim(cntry)
        end as entry
    from data_warehousing.bronze.erp_loc_a101;
    
    ---- loading erp_px_cat_g1v2
    truncate table data_warehousing.silver.erp_px_cat_g1v2;
    
    insert into data_warehousing.silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    select id, cat, subcat, maintenance from data_warehousing.bronze.erp_px_cat_g1v2;
    
    ----------------
    return 'All ERP loaded from bronze to silver';
end;

call DATA_WAREHOUSING.SILVER.LOAD_CRM_BRONZE_TO_SILVER();

call DATA_WAREHOUSING.SILVER.LOAD_ERP_BRONZE_TO_SILVER();
