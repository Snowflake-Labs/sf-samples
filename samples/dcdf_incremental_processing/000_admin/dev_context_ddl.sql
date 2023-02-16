------------------------------------------------------------------------------
-- context
--
!print set environment context

!define l_common_db=DEV_WEBINAR_COMMON_DB
!define l_common_schema=UTIL

!define l_raw_db=DEV_WEBINAR_ORDERS_RL_DB
!define l_raw_schema=tpch

!define l_il_db=DEV_WEBINAR_IL_DB
!define l_il_schema=main

!define l_pl_db=DEV_WEBINAR_PL_DB
!define l_pl_schema=main

create database if not exists &{l_common_db};
create schema if not exists &{l_common_db}.&{l_common_schema};

create database if not exists &{l_raw_db};
create schema if not exists &{l_raw_db}.&{l_raw_schema};

create database if not exists &{l_il_db};
create schema if not exists &{l_il_db}.&{l_il_schema};

create database if not exists &{l_pl_db};
create schema if not exists &{l_pl_db}.&{l_pl_schema};

create warehouse if not exists DEV_WEBINAR_WH warehouse_size=small;

!print SUCCESS!
