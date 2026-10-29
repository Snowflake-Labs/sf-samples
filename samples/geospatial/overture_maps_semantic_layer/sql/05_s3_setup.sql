create file format if not exists $ns.OVERTURE_PARQUET
type = parquet binary_as_text = false use_logical_type = true
comment = $owner_comment;

create stage if not exists $ns.OVERTURE_S3
url = 's3://overturemaps-us-west-2/release/'
$integration
file_format = $ns.OVERTURE_PARQUET
comment = $owner_comment;