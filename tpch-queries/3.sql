-- TPC-H/TPC-R Shipping Priority Query (Q3) - Spark SQL Compatible

SELECT
	l_orderkey,
	SUM(l_extendedprice * (1 - l_discount)) AS revenue,
	o_orderdate,
	o_shippriority
FROM
	customer c
	JOIN orders o ON c.c_custkey = o.o_custkey
	JOIN lineitem l ON l.l_orderkey = o.o_orderkey
WHERE
	c.c_mktsegment = ':1'
	AND o.o_orderdate < DATE ':2'
	AND l.l_shipdate > DATE ':2'
GROUP BY
	l_orderkey,
	o_orderdate,
	o_shippriority
ORDER BY
	revenue DESC,
	o_orderdate
LIMIT 10
