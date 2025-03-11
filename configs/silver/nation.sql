SELECT 
    CAST(n_nationkey AS BIGINT) as n_nationkey,
    CAST(n_name AS VARCHAR(25)) as n_name,
    CAST(n_regionkey AS BIGINT) as n_regionkey,
    CAST(n_comment AS VARCHAR(152)) as n_comment
FROM practice_sandbox.ma_sandbox.bronze_nation
