CREATE OR REPLACE VIEW practice_sandbox.ma_sandbox.gold_dim_part AS
SELECT 
    p_partkey as part_key,
    TRIM(p_name) as part_name,
    TRIM(p_mfgr) as manufacturer,
    TRIM(p_brand) as brand,
    TRIM(p_type) as part_type,
    p_size as part_size,
    TRIM(p_container) as container_type,
    p_retailprice as retail_price,
    p_comment as part_comment,
    current_timestamp() as silver_update_timestamp
FROM practice_sandbox.ma_sandbox.silver_part
