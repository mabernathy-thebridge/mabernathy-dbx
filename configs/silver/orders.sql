SELECT 
    CAST(o_orderkey AS BIGINT) as o_orderkey,
    CAST(o_custkey AS BIGINT) as o_custkey,
    CAST(o_orderstatus AS VARCHAR(1)) as o_orderstatus,
    CAST(o_totalprice AS DECIMAL(12,2)) as o_totalprice,
    CAST(o_orderdate AS DATE) as o_orderdate,
    CAST(o_orderpriority AS VARCHAR(15)) as o_orderpriority,
    CAST(o_clerk AS VARCHAR(15)) as o_clerk,
    CAST(o_shippriority AS INT) as o_shippriority,
    CAST(o_comment AS VARCHAR(79)) as o_comment
FROM practice_sandbox.ma_sandbox.bronze_orders
