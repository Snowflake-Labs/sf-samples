with c1 as (
    select 
        distinct s.fiscper sfiscper
    from 
        {{ ref('en_0fi_ar_4') }} s
    where 
        fiscper not in (
            select fiscper
            from {{ ref('currency') }}
        )
),
c2 as (
    select 
        distinct sfiscper,
        fcurr,
        tcurr,
        ffact,
        tfact,
        t.fiscper tfiscper,
        ukurs,
        case
            when row_number() over (
                partition by sfiscper, fcurr, tcurr
                order by tfiscper desc
            ) = 1 then 'previous'
        end ref_fiscper
    from {{ ref('currency') }} t
        join c1 on c1.sfiscper > t.fiscper
),
c3 as (
    select 
        distinct sfiscper,
        fcurr,
        tcurr,
        ffact,
        tfact,
        t.fiscper tfiscper,
        ukurs,
        case
            when row_number() over (
                partition by sfiscper, fcurr, tcurr
                order by t.fiscper
            ) = 1 then 'next'
        end ref_fiscper
    from {{ ref('currency') }} t
    join c1 on c1.sfiscper < t.fiscper
),
c4 as (
    select *
    from c2
    where ref_fiscper = 'previous'
    union
    select *
    from c3
    WHERE REF_FISCPER = 'next'

),
c5 as (
    select *,
    case 
        when tfiscper = sfiscper -1 
        or tfiscper = sfiscper -989 then 
        -1
        when tfiscper > sfiscper then 
        1
    end chosen
    from c4
),
c6 as (
    select *,
    case
        when row_number() over (
            partition by sfiscper, fcurr, tcurr
            order by chosen
        ) = 1 then tfiscper
    end G
    from c5
)
select sfiscper,
    tfiscper,
    fcurr,
    tcurr,
    ffact,
    tfact,
    case
        when ukurs < 0 then -1 / ukurs
        else ukurs
    end ukurs
from c6
where G is not null
union
select 
    distinct s.fiscper sfiscper,
    t.fiscper tfiscper,
    fcurr,
    tcurr,
    ffact,
    tfact,
    min(
        case
            when ukurs < 0 
                then -1 / ukurs
            else ukurs
        end
    ) ukurs
from {{ ref('en_0fi_ar_4') }} s
    join {{ ref('currency') }} t on s.fiscper = t.fiscper
{{ dbt_utils.group_by(6) }}
