-- TPC-H/TPC-R Product Type Profit Measure Query (Q9) - Spark SQL Compatible

SELECT
	nation,
	o_year,
	SUM(amount) AS sum_profit
FROM (
	SELECT
		n_name AS nation,
		YEAR(o_orderdate) AS o_year,
		l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
	FROM
		part
		JOIN partsupp ON part.p_partkey = partsupp.ps_partkey
		JOIN lineitem ON lineitem.l_partkey = part.p_partkey AND lineitem.l_suppkey = partsupp.ps_suppkey
		JOIN supplier ON supplier.s_suppkey = lineitem.l_suppkey
		JOIN orders ON orders.o_orderkey = lineitem.l_orderkey
		JOIN nation ON supplier.s_nationkey = nation.n_nationkey
	WHERE
		part.p_name LIKE '%:1%'
) profit
GROUP BY
	nation,
	o_year
ORDER BY
	nation,
	o_year DESC
