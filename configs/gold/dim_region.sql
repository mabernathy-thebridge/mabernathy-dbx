CREATE OR REPLACE VIEW practice_sandbox.ma_sandbox.gold_dim_region AS
SELECT 
    r_regionkey as region_key,
    TRIM(r_name) as region_name,
    r_comment as region_comment,
    current_timestamp() as silver_update_timestamp
FROM practice_sandbox.ma_sandbox.silver_region
