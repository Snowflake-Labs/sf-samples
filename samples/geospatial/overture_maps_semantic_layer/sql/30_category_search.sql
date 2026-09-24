create table if not exists $ns.CATEGORY_VOCAB
change_tracking = true data_retention_time_in_days = 1 comment = $owner_comment as
select category, count(*) as place_count,
       replace(category, '_', ' ') || ' (Overture primary category: ' || category || ')'
           as search_text
from $ns.PLACE
where category is not null
group by category;

create cortex search service if not exists $ns.CATEGORY_SEARCH
on search_text attributes category
warehouse = $warehouse target_lag = '12 hours'
comment = $owner_comment
as select category, place_count, search_text from $ns.CATEGORY_VOCAB;