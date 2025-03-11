SELECT 
    CAST(p_partkey AS BIGINT) as p_partkey,
    CAST(p_name AS VARCHAR(55)) as p_name,
    CAST(p_mfgr AS VARCHAR(25)) as p_mfgr,
    CAST(p_brand AS VARCHAR(10)) as p_brand,
    CAST(p_type AS VARCHAR(25)) as p_type,
    CAST(p_size AS INT) as p_size,
    CAST(p_container AS VARCHAR(10)) as p_container,
    CAST(p_retailprice AS DECIMAL(12,2)) as p_retailprice,
    CAST(p_comment AS VARCHAR(23)) as p_comment
FROM practice_sandbox.ma_sandbox.bronze_part
