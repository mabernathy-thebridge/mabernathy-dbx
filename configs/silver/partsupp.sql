eSELECT 
    CAST(ps_partkey AS BIGINT) as ps_partkey,
    CAST(ps_suppkey AS BIGINT) as ps_suppkey,
    CAST(ps_availqty AS INT) as ps_availqty,
    CAST(ps_supplycost AS DECIMAL(12,2)) as ps_supplycost,
    CAST(ps_comment AS VARCHAR(199)) as ps_comment
FROM practice_sandbox.ma_sandbox.bronze_partsupp
