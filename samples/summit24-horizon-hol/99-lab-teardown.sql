
/***************************************************************************************************
| H | O | R | I | Z | O | N |   | L | A | B | S | 

Demo:         Horizon Lab
Version:      HLab v1
Create Date:  Apr 17, 2024
Author:       Ravi Kumar
Co-Authors:    Ben Weiss, Susan Devitt
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************/
/****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
Apr 17, 2024        Ravi Kumar           Initial Lab
***************************************************************************************************/

/********************/
-- T E A R   D O W N
/********************/
USE ROLE HRZN_DATA_ENGINEER;
USE WAREHOUSE HRZN_WH;

DROP TABLE IF EXISTS HRZN_DB.HRZN_SCH.CUSTOMER;
DROP TABLE IF EXISTS HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS;
DROP SCHEMA IF EXISTS HRZN_DB.HRZN_SCH;


USE ROLE HRZN_DATA_GOVERNOR;
DROP SCHEMA IF EXISTS HRZN_DB.CLASSIFIERS;
DROP TABLE IF EXISTS HRZN_DB.TAG_SCHEMA.ROW_POLICY_MAP;
DROP SCHEMA IF EXISTS HRZN_DB.TAG_SCHEMA;
DROP SCHEMA IF EXISTS HRZN_DB.SEC_POLICIES_SCHEMA;


USE ROLE HRZN_DATA_ENGINEER;
DROP DATABASE IF EXISTS HRZN_DB;


USE ROLE SECURITYADMIN;
DROP ROLE IF EXISTS HRZN_DATA_GOVERNOR;
DROP ROLE IF EXISTS HRZN_DATA_USER;
DROP ROLE IF EXISTS HRZN_IT_ADMIN;
DROP ROLE IF EXISTS HRZN_DATA_ENGINEER;

USE ROLE SYSADMIN;
DROP WAREHOUSE IF EXISTS HRZN_WH;



