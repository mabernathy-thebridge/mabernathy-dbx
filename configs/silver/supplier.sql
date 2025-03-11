SELECT 
    CAST(s_suppkey AS BIGINT) as s_suppkey,
    CAST(s_name AS VARCHAR(25)) as s_name,
    CAST(s_address AS VARCHAR(40)) as s_address,
    CAST(s_nationkey AS BIGINT) as s_nationkey,
    CAST(s_phone AS VARCHAR(15)) as s_phone,
    CAST(s_acctbal AS DECIMAL(12,2)) as s_acctbal,
    CAST(s_comment AS VARCHAR(101)) as s_comment
FROM practice_sandbox.ma_sandbox.bronze_supplier
