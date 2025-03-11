SELECT 
    r_regionkey as region_key,
    TRIM(r_name) as region_name,
    r_comment as region_comment,
    current_timestamp() as silver_update_timestamp
FROM practice_sandbox.ma_sandbox.region
