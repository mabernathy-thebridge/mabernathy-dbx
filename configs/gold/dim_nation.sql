SELECT 
    n.n_nationkey as nation_key,
    TRIM(n.n_name) as nation_name,
    n.n_regionkey as region_key,
    TRIM(r.r_name) as region_name,
    n.n_comment as nation_comment,
    current_timestamp() as silver_update_timestamp
FROM practice_sandbox.ma_sandbox.silver_nation n
LEFT JOIN practice_sandbox.ma_sandbox.silver_region r
    ON n.n_regionkey = r.r_regionkey
