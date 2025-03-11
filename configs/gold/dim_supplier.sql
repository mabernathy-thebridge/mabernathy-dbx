SELECT 
    s.s_suppkey as supplier_key,
    TRIM(s.s_name) as supplier_name,
    TRIM(s.s_address) as supplier_address,
    s.s_nationkey as nation_key,
    TRIM(n.n_name) as nation_name,
    REGEXP_REPLACE(s.s_phone, '[^0-9]', '') as supplier_phone_clean,
    s.s_acctbal as account_balance,
    s.s_comment as supplier_comment,
    current_timestamp() as silver_update_timestamp
FROM practice_sandbox.ma_sandbox.silver_supplier s
LEFT JOIN practice_sandbox.ma_sandbox.silver_nation n
    ON s.s_nationkey = n.n_nationkey
