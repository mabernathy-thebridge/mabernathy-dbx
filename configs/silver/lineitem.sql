SELECT 
    CAST(l_orderkey AS BIGINT) as l_orderkey,
    CAST(l_partkey AS BIGINT) as l_partkey,
    CAST(l_suppkey AS BIGINT) as l_suppkey,
    CAST(l_linenumber AS INT) as l_linenumber,
    CAST(l_quantity AS DECIMAL(12,2)) as l_quantity,
    CAST(l_extendedprice AS DECIMAL(12,2)) as l_extendedprice,
    CAST(l_discount AS DECIMAL(12,2)) as l_discount,
    CAST(l_tax AS DECIMAL(12,2)) as l_tax,
    CAST(l_returnflag AS VARCHAR(1)) as l_returnflag,
    CAST(l_linestatus AS VARCHAR(1)) as l_linestatus,
    CAST(l_shipdate AS DATE) as l_shipdate,
    CAST(l_commitdate AS DATE) as l_commitdate,
    CAST(l_receiptdate AS DATE) as l_receiptdate,
    CAST(l_shipinstruct AS VARCHAR(25)) as l_shipinstruct,
    CAST(l_shipmode AS VARCHAR(10)) as l_shipmode,
    CAST(l_comment AS VARCHAR(44)) as l_comment
FROM practice_sandbox.ma_sandbox.bronze_lineitem
