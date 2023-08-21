with access_history_flatten as (
    select
        r.value:"objectId" as source_id,
        r.value:"objectName" as source_name,
        r.value:"objectDomain" as source_domain,
        w.value:"objectId" as target_id,
        w.value:"objectName" as target_name,
        w.value:"objectDomain" as target_domain,
        c.value:"columnName" as target_column,
        t.query_start_time as query_start_time
    from
        (select * from snowflake.ACCOUNT_USAGE.ACCESS_HISTORY) t,
        lateral flatten(input => t.BASE_OBJECTS_ACCESSED) r,
        lateral flatten(input => t.OBJECTS_MODIFIED) w,
        lateral flatten(input => w.value:"columns", outer => true) c
        ),
    sensitive_data_movements(path, target_id, target_name, target_domain, target_column, query_start_time)
    as
      -- Common Table Expression
      (
        -- Anchor Clause: Get the objects that access S1 directly
        select
            f.source_name || '-->' || f.target_name as path,
            f.target_id,
            f.target_name,
            f.target_domain,
            f.target_column,
            f.query_start_time
        from
            access_history_flatten f
        where
        f.source_domain = 'Stage'
        --and f.source_name = 'TEST_DB.TEST_SCHEMA.S1'
        and f.query_start_time >= dateadd(day, -30, date_trunc(day, current_date))
        union all
        -- Recursive Clause: Recursively get all the objects that access S1 indirectly
        select sensitive_data_movements.path || '-->' || f.target_name as path, f.target_id, f.target_name, f.target_domain, f.target_column, f.query_start_time
          from
             access_history_flatten f
            join sensitive_data_movements
            on f.source_id = sensitive_data_movements.target_id
                and f.source_domain = sensitive_data_movements.target_domain
                and f.query_start_time >= sensitive_data_movements.query_start_time
      )
select path, target_name, target_id, target_domain, array_agg(distinct target_column) as target_columns
from sensitive_data_movements
group by path, target_id, target_name, target_domain
limit 100;