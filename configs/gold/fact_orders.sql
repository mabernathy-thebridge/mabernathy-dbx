CREATE OR REPLACE VIEW practice_sandbox.ma_sandbox.gold_fact_orders AS
SELECT 
    o.o_orderkey as order_key,
    o.o_custkey as customer_key,
    UPPER(o.o_orderstatus) as order_status,
    o.o_totalprice as total_price,
    CAST(o.o_orderdate as DATE) as order_date,
    TRIM(o.o_orderpriority) as order_priority,
    TRIM(o.o_clerk) as clerk,
    o.o_shippriority as shipping_priority,
    o.o_comment as order_comment,
    current_timestamp() as silver_update_timestamp
FROM practice_sandbox.ma_sandbox.silver_orders o
