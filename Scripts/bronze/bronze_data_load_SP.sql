/*
===============================================================================
Stored Procedure: Load Bronze Layer (Snowflake internal stage -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from CSV files (loaded in Snowflake internal stage). 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters.
    Return a String with success mgs.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


use database data_warehousing;
use schema bronze;
----------------------------------------------
-- view data in stages
list @"DATA_WAREHOUSING"."BRONZE"."CRM_STAGE";
list @"DATA_WAREHOUSING"."BRONZE"."ERP_STAGE";

-----------------------------------------------
-- Stored procedure for loading data from stage files to table
create or replace PROCEDURE load_CRM_stage_to_bronze()
returns string
language SQL
as
begin
    -- copy data from stage to table for crm_cust_info
    truncate table data_warehousing.bronze.crm_cust_info;
    COPY INTO data_warehousing.bronze.crm_cust_info from '@"DATA_WAREHOUSING"."BRONZE"."CRM_STAGE"/cust_info.csv'
    FILE_FORMAT = (type = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 TRIM_SPACE = TRUE)
    ON_ERROR = ABORT_STATEMENT;

    -- copy data from stage to table for crm_prd_info
    truncate table data_warehousing.bronze.crm_prd_info;
    copy into data_warehousing.bronze.crm_prd_info
    from @"DATA_WAREHOUSING"."BRONZE"."CRM_STAGE"/prd_info.csv
    file_format = (type = CSV skip_header = 1 field_delimiter = ',')
    on_error = abort_statement;

    -- copy data from stage to table for crm_sales_details
    truncate table data_warehousing.bronze.crm_sales_details;
    copy into data_warehousing.bronze.crm_sales_details
    from @"DATA_WAREHOUSING"."BRONZE"."CRM_STAGE"/sales_details.csv
    file_format = (type=CSV skip_header=1 field_delimiter=',')
    on_error=abort_statement;

    return 'All CRM data loaded sucessful in bronze layer.';
end;



--------------------------------------------------
-- Create a file format to use in loading data for alternat method
create or replace file format csv_file_format_with_header1
type = CSV field_delimiter= ',' skip_header=1;

-- create a stored procedure to load all ERP data from sage to bronze layer
create or replace procedure load_ERP_stage_to_table()
returns string
language sql
as
begin
    -- load erp_cust_az12 table
    truncate table data_warehousing.bronze.erp_cust_az12;
    
    copy into data_warehousing.bronze.erp_cust_az12
    from ( select $1, $2, $3 from @"DATA_WAREHOUSING"."BRONZE"."ERP_STAGE"/ERP/CUST_AZ12.csv)
    file_format = (type=CSV skip_header=1 field_delimiter=',')
    on_error=abort_statement;

    -- load erp_loc_a101 table
    truncate table data_warehousing.bronze.erp_loc_a101;
    
    copy into data_warehousing.bronze.erp_loc_a101
    from (select $1, $2 from @"DATA_WAREHOUSING"."BRONZE"."ERP_STAGE"/ERP/LOC_A101.csv)
    file_format = (format_name = csv_file_format_with_header1)
    on_error = abort_statement;

    -- load erp_px_cat_g1v2 table
    truncate table data_warehousing.bronze.erp_px_cat_g1v2;
    
    copy into data_warehousing.bronze.erp_px_cat_g1v2
    from (select $1, $2, $3, $4 from @"DATA_WAREHOUSING"."BRONZE"."ERP_STAGE"/ERP/)
    files = ('PX_CAT_G1V2.csv')
    file_format = (format_name=csv_file_format_with_header1)
    on_error = abort_statement;

    return 'All ERP tables loaded sucessfully to bromze layer.';
end;


-------------------------------------
-- Excute stored procedure
call load_CRM_stage_to_bronze();
call load_erp_stage_to_table();

