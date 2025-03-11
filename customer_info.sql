SELECT * 
FROM practice_sandbox.ma_sandbox.lineitem li
JOIN practice_sandbox.ma_sandbox.orders o ON li.l_orderkey = o.o_orderkey
JOIN practice_sandbox.ma_sandbox.customer c ON o.o_custkey = c.c_custkey