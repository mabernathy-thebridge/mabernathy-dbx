SELECT 
    c.c_custkey as customer_key,
    TRIM(c.c_name) as customer_name,
    TRIM(c.c_address) as customer_address,
    c.c_nationkey as nation_key,
    TRIM(n.n_name) as nation_name,
    REGEXP_REPLACE(c.c_phone, '[^0-9]', '') as customer_phone_clean,
    c.c_acctbal as account_balance,
    TRIM(c.c_mktsegment) as market_segment,
    c.c_comment as customer_comment,
    current_timestamp() as silver_update_timestamp
FROM practice_sandbox.ma_sandbox.silver_customer c
LEFT JOIN practice_sandbox.ma_sandbox.silver_nation n
    ON c.c_nationkey = n.n_nationkey
