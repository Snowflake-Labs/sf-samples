create table $candidate comment = $owner_comment as
select $projection
from @$ns.OVERTURE_S3/$release/theme=$theme/type=$type/
$filter;