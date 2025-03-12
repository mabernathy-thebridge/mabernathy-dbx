WITH customer_orders AS (
    SELECT 
        c.c_custkey,
        c.c_mktsegment,
        c.c_nationkey,
        o.o_orderkey,
        o.o_orderdate,
        o.o_totalprice,
        l.l_quantity,
        l.l_extendedprice,
        p.p_type
    FROM practice_sandbox.ma_sandbox.silver_customer c
    LEFT JOIN practice_sandbox.ma_sandbox.silver_orders o
        ON c.c_custkey = o.o_custkey
    LEFT JOIN practice_sandbox.ma_sandbox.silver_lineitem l
        ON o.o_orderkey = l.l_orderkey
    LEFT JOIN practice_sandbox.ma_sandbox.silver_part p
        ON l.l_partkey = p.p_partkey
),

customer_metrics AS (
    SELECT 
        c_custkey,
        c_mktsegment,
        c_nationkey,
        COUNT(DISTINCT o_orderkey) as total_orders,
        SUM(o_totalprice) as total_spent,
        AVG(o_totalprice) as avg_order_value,
        MAX(o_orderdate) as last_order_date,
        MIN(o_orderdate) as first_order_date,
        datediff(MAX(o_orderdate), MIN(o_orderdate)) as customer_lifetime_days,
        SUM(l_quantity) as total_items_purchased,
        COUNT(DISTINCT p_type) as unique_product_types
    FROM customer_orders
    GROUP BY 1, 2, 3
)
SELECT 
    cm.*,
    n.n_name as nation_name,
    r.r_name as region_name,
    CASE 
        WHEN total_spent > 10000000 THEN 'High Value'
        WHEN total_spent > 5000000 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_segment,
    current_timestamp() as model_timestamp
FROM customer_metrics cm
LEFT JOIN practice_sandbox.ma_sandbox.silver_nation n
    ON cm.c_nationkey = n.n_nationkey
LEFT JOIN practice_sandbox.ma_sandbox.silver_region r
    ON n.n_regionkey = r.r_regionkey
ORDER BY total_spent desc

