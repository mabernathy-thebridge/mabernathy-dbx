







FROM practice_sandbox.ma_sandbox.partsupp    current_timestamp() as silver_update_timestamp    ps_comment as partsupp_comment,    ps_supplycost as supply_cost,    ps_availqty as available_quantity,    ps_suppkey as supplier_key,    ps_partkey as part_key,SELECT 