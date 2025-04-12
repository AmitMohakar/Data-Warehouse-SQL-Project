/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, replacing existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables

    Create internal stage in Snowflake to store data files.
===============================================================================
*/

create or replace table DATA_WAREHOUSING.bronze.crm_cust_info 
(
    cst_id int,
    cst_key varchar,
    cst_firstname varchar,
    cst_lastname varchar,
    cst_marital_status varchar,
    cst_gndr varchar,
    cst_create_date date
);


create or replace table data_warehousing.bronze.crm_prd_info
(
    prd_id int,
    prd_key varchar,
    prd_nm varchar,
    prd_cost int,
    prd_line varchar,
    prd_start_dt timestamp,
    prd_end_dt timestamp
);


create or replace table data_warehousing.bronze.crm_sales_details
(
    sls_ord_num varchar,
    sls_prd_key varchar,
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int
);


create or replace table data_warehousing.bronze.erp_CUST_AZ12
(
    CID varchar,
    BDATE date,
    GEN varchar
);


create or replace table data_warehousing.bronze.erp_LOC_A101
(
    CID	varchar,
    CNTRY varchar
);

create or replace table data_warehousing.bronze.erp_PX_CAT_G1V2
(
    ID varchar,
    CAT varchar,
    SUBCAT varchar,
    MAINTENANCE varchar
);

-----------------------------------------------
-- create internal stage 
create or replace stage data_warehousing.bronze.CRM_stage
file_format = (type = CSV)
comment = 'This is the stage for CRM files';

create or replace stage data_warehousing.bronze.ERP_stage
file_format = (type = CSV)
comment = 'This is the stage for ERP files';

--// upload files to the stage manualy

--list all files in stage
list @"DATA_WAREHOUSING"."BRONZE"."CRM_STAGE";
