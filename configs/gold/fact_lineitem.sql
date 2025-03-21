CREATE OR REPLACE VIEW practice_sandbox.ma_sandbox.gold_fact_lineitem AS
SELECT 
    l_orderkey as order_key,
    l_partkey as part_key,
    l_suppkey as supplier_key,
    l_linenumber as line_number,
    l_quantity as quantity,
    l_extendedprice as extended_price,
    l_discount as discount,
    l_tax as tax,
    l_extendedprice * (1 - l_discount) * (1 + l_tax) as final_price,
    UPPER(l_returnflag) as return_flag,
    UPPER(l_linestatus) as line_status,
    CAST(l_shipdate as DATE) as ship_date,
    CAST(l_commitdate as DATE) as commit_date,
    CAST(l_receiptdate as DATE) as receipt_date,
    TRIM(l_shipinstruct) as ship_instructions,
    TRIM(l_shipmode) as ship_mode,
    l_comment as line_comment,
    current_timestamp() as silver_update_timestamp
FROM practice_sandbox.ma_sandbox.silver_lineitem
