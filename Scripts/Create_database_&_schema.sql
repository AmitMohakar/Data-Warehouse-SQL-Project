/*
=============================================================
Create Database, Schemas and data warehouse
=============================================================
Script Purpose:
    This script will create a new data warehouse named 'data_engine'.
    This script creates a new database named 'data_warehousing' if not exists exists. 
    If the database exists, it will keep it. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
*/

create database if not exists data_warehousing;

create warehouse if not exists data_engine
with 
warehouse_size = xsmall
auto_suspend = 120
auto_resume = True;

use database data_warehousing;

create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;
