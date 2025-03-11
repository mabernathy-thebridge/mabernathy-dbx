SELECT
    ps_partkey as part_key,
    ps_suppkey as supplier_key,
    ps_availqty as available_quantity,
    ps_supplycost as supply_cost,
    ps_comment as partsupp_comment,
    current_timestamp() as silver_update_timestamp
FROM practice_sandbox.ma_sandbox.silver_partsupp