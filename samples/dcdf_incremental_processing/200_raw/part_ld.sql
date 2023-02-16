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

/* Validation Queries
truncate table part;

select * from part limit 1000;

all 3 counts should be identical

select count(*), count( distinct p_partkey ), min( o_orderdate ), max( o_orderdate ) from part_stg;

select count(*), count( distinct dw_part_shk ), count( distinct dw_hash_diff ) from part;
*/

execute immediate $$

begin
    
    --
    -- Merge Pattern
    --
    merge into part tgt using
    (
        with l_stg as
        (
            --
            -- Driving CTE to identify all records in the logical partition to be processed
            --
            select
                -- generate hash key and hash diff to streamline processing
                 sha1_binary( s.p_partkey )  as dw_part_shk
                -- note that last_modified_dt is not included in the hash diff since it only represents recency of the record versus an 
                -- actual meaningful change in the data
                ,sha1_binary( concat( s.p_partkey
                                     ,'|', coalesce( s.p_name, '~' )
                                     ,'|', coalesce( s.p_mfgr, '~' )
                                     ,'|', coalesce( s.p_brand, '~' )
                                     ,'|', coalesce( s.p_type, '~' )
                                     ,'|', coalesce( to_char( s.p_size ), '~' )
                                     ,'|', coalesce( s.p_container, '~' )
                                     ,'|', coalesce( to_char( s.p_retailprice ), '~' )
                                     ,'|', coalesce( s.p_comment, '~' )
                                    )
            
                            )               as dw_hash_diff
                ,s.*
            from
                part_stg s
        )
        ,l_deduped as
        (
            -- 
            -- Dedupe the records from the staging table.
            -- This assumes that there may be late arriving or duplicate data that were loaded
            -- Need to identify the most recent record and use that to update the Current state table.
            -- as there is no reason to process each individual change in the record, the last one would have the most recent updates
            select
                *
            from
                l_stg s
            qualify
                row_number() over( partition by dw_part_shk order by s.last_modified_dt desc ) = 1
        )
        ,l_tgt as
        (
            --
            -- Its own CTE, for partition pruning efficiencies
            select *
            from part
        )
        select
             current_timestamp()        as dw_version_ts
            ,s.*
        from
            l_deduped s
            left join l_tgt t on
                t.dw_part_shk = s.dw_part_shk
        where
            -- source row does not exist in target table
            t.dw_part_shk is null
            -- or source row is more recent and differs from target table
            or (
                    t.last_modified_dt  < s.last_modified_dt
                and t.dw_hash_diff     != s.dw_hash_diff
               )
        order by
            s.p_partkey
    ) src
    on
    (
        tgt.dw_part_shk = src.dw_part_shk
    )
    when matched then update set
         tgt.dw_hash_diff      = src.dw_hash_diff
        ,tgt.dw_version_ts     = src.dw_version_ts
        ,tgt.p_name            = src.p_name
        ,tgt.p_mfgr            = src.p_mfgr
        ,tgt.p_brand           = src.p_brand
        ,tgt.p_type            = src.p_type
        ,tgt.p_size            = src.p_size
        ,tgt.p_container       = src.p_container
        ,tgt.p_retailprice     = src.p_retailprice
        ,tgt.p_comment         = src.p_comment
        ,tgt.last_modified_dt  = src.last_modified_dt
        ,tgt.dw_file_name      = src.dw_file_name
        ,tgt.dw_file_row_no    = src.dw_file_row_no
        ,tgt.dw_update_ts      = src.dw_version_ts
    when not matched then insert
    (
         dw_part_shk
        ,dw_hash_diff
        ,dw_version_ts
        ,p_partkey
        ,p_name
        ,p_mfgr
        ,p_brand
        ,p_type
        ,p_size
        ,p_container
        ,p_retailprice
        ,p_comment
        ,last_modified_dt
        ,dw_file_name
        ,dw_file_row_no
        ,dw_load_ts
        ,dw_update_ts
    )
    values
    (
         src.dw_part_shk
        ,src.dw_hash_diff
        ,src.dw_version_ts
    	,src.p_partkey
    	,src.p_name
    	,src.p_mfgr
    	,src.p_brand
    	,src.p_type
    	,src.p_size
    	,src.p_container
    	,src.p_retailprice
    	,src.p_comment
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


select *
from dev_webinar_orders_rl_db.tpch.part
where p_partkey in ( 105237594, 128236374);