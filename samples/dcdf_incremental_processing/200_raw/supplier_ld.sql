--------------------------------------------------------------------
--  Purpose: merge new and modified values into target table
--      This shows a merge pattern where only the most recent version
--      of a given primary keys record is kept.
--
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     sysadmin;
use database dev_webinar_orders_rl_db;
use schema   tpch;
use warehouse dev_webinar_wh;

/*
truncate table supplier;

select * from supplier limit 1000;

all 3 counts should be identical

select count(*), count( distinct s_suppkey ), min( o_orderdate ), max( o_orderdate ) from supplier_stg;

select count(*), count( distinct dw_supplier_shk ), count( distinct dw_hash_diff ) from supplier;
*/

execute immediate $$

begin
    
    --
    -- update modified and insert new
    --
    merge into supplier tgt using
    (
        with l_stg as
        (
            select
                -- generate hash key and hash diff to streamline processing
                 sha1_binary( s.s_suppkey )  as dw_supplier_shk
                -- note that last_modified_dt is not included in the hash diff since it only represents recency of the record versus an 
                -- actual meaningful change in the data
                ,sha1_binary( concat( s.s_suppkey
                                     ,'|', coalesce( s.s_name, '~' )
                                     ,'|', coalesce( s.s_address, '~' )
                                     ,'|', coalesce( to_char( s.s_nationkey ), '~' )
                                     ,'|', coalesce( s.s_phone, '~' )
                                     ,'|', coalesce( to_char( s.s_acctbal ), '~' )
                                     ,'|', coalesce( s.s_comment, '~' )
                                    )
            
                            )               as dw_hash_diff
                ,s.*
            from
                supplier_stg s
        )
        ,l_deduped as
        (
            select
                *
            from
                l_stg s
            qualify
                row_number() over( partition by dw_supplier_shk order by s.last_modified_dt desc ) = 1
        )
        ,l_tgt as
        (
            select *
            from supplier
        )
        select
             current_timestamp()        as dw_version_ts
            ,s.*
        from
            l_deduped s
            left join l_tgt t on
                t.dw_supplier_shk = s.dw_supplier_shk
        where
            -- source row does not exist in target table
            t.dw_supplier_shk is null
            -- or source row is more recent and differs from target table
            or (
                    t.last_modified_dt  < s.last_modified_dt
                and t.dw_hash_diff     != s.dw_hash_diff
               )
        order by
            s.s_suppkey
    ) src
    on
    (
        tgt.dw_supplier_shk = src.dw_supplier_shk
    )
    when matched then update set
         tgt.dw_hash_diff      = src.dw_hash_diff
        ,tgt.dw_version_ts     = src.dw_version_ts
        ,tgt.s_name            = src.s_name
        ,tgt.s_address         = src.s_address
        ,tgt.s_nationkey       = src.s_nationkey
        ,tgt.s_phone           = src.s_phone
        ,tgt.s_acctbal         = src.s_acctbal
        ,tgt.s_comment         = src.s_comment
        ,tgt.last_modified_dt  = src.last_modified_dt
        ,tgt.dw_file_name      = src.dw_file_name
        ,tgt.dw_file_row_no    = src.dw_file_row_no
        ,tgt.dw_update_ts      = src.dw_version_ts
    when not matched then insert
    (
         dw_supplier_shk
        ,dw_hash_diff
        ,dw_version_ts
	,s_suppkey
	,s_name
	,s_address
	,s_nationkey
	,s_phone
	,s_acctbal
	,s_comment
        ,last_modified_dt
        ,dw_file_name
        ,dw_file_row_no
        ,dw_load_ts
        ,dw_update_ts
    )
    values
    (
         src.dw_supplier_shk
        ,src.dw_hash_diff
        ,src.dw_version_ts
	,src.s_suppkey
	,src.s_name
	,src.s_address
	,src.s_nationkey
	,src.s_phone
	,src.s_acctbal
	,src.s_comment
        ,src.last_modified_dt
        ,src.dw_file_name
        ,src.dw_file_row_no
        ,src.dw_load_ts
        ,src.dw_version_ts
    )
    ;

  return 'SUCCESS';

end;
$$
;

