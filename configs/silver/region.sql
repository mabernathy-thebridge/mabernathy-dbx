SELECT 
    CAST(r_regionkey AS BIGINT) as r_regionkey,
    CAST(r_name AS VARCHAR(25)) as r_name,
    CAST(r_comment AS VARCHAR(152)) as r_comment
FROM practice_sandbox.ma_sandbox.bronze_region
