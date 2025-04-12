/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, replace existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/
create or replace table data_warehousing.silver.crm_cust_info
(   cst_id int,
    cst_key varchar,
    cst_firstname varchar,
    cst_lastname varchar,
    cst_marital_status varchar,
    cst_gndr varchar,
    cst_create_date date,
    dwh_create_date timestamp default current_timestamp()
);

create or replace table data_warehousing.silver.crm_prd_info
(   prd_id int,
    cat_id varchar,
    prd_key varchar,
    prd_nm varchar,
    prd_cost int,
    prd_line varchar,
    prd_start_dt date,
    prd_end_dt date,
    dwh_create_date timestamp default current_timestamp()
);


create or replace table data_warehousing.silver.crm_sales_details
(
    sls_ord_num varchar,
    sls_prd_key varchar,
    sls_cust_id int,
    sls_order_dt date,
    sls_ship_dt date,
    sls_due_dt date,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_date timestamp default current_timestamp()
);

create or replace table data_warehousing.silver.ERP_cust_AZ12
(
    cid varchar,
    bdate date,
    gen varchar,
    dwh_create_date timestamp default current_timestamp()
);


create or replace table data_warehousing.silver.erp_loc_a101
(
    cid varchar,
    cntry varchar,
    dwh_create_date timestamp default current_timestamp()
);


create or replace table data_warehousing.silver.erp_px_cat_g1v2
(
    id varchar,
    cat varchar,
    subcat varchar,
    maintenance varchar,
    dwh_create_date timestamp default current_timestamp()
);
