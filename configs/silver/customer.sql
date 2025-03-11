SELECT 
    CAST(c_custkey AS BIGINT) as c_custkey,
    CAST(c_name AS VARCHAR(25)) as c_name,
    CAST(c_address AS VARCHAR(40)) as c_address,
    CAST(c_nationkey AS BIGINT) as c_nationkey,
    CAST(c_phone AS VARCHAR(15)) as c_phone,
    CAST(c_acctbal AS DECIMAL(12,2)) as c_acctbal,
    CAST(c_mktsegment AS VARCHAR(10)) as c_mktsegment,
    CAST(c_comment AS VARCHAR(117)) as c_comment
FROM practice_sandbox.ma_sandbox.bronze_customer
