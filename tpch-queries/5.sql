-- TPC-H/TPC-R Local Supplier Volume Query (Q5) - Spark SQL Compatible

SELECT
	n_name,
	SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
	customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey AND c_nationkey = s_nationkey
JOIN region ON n_regionkey = r_regionkey
WHERE
	r_name = '${region_name}'
	AND o_orderdate >= DATE '${start_date}'
	AND o_orderdate < DATE_ADD(DATE '${start_date}', 365)
GROUP BY
	n_name
ORDER BY
	revenue DESC
