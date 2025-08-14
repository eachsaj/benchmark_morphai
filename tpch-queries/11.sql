-- TPC-H/TPC-R Important Stock Identification Query (Q11) - Spark SQL Compatible

SELECT
	ps_partkey,
	SUM(ps_supplycost * ps_availqty) AS value
FROM
	partsupp
JOIN
	supplier ON ps_suppkey = s_suppkey
JOIN
	nation ON s_nationkey = n_nationkey
WHERE
	n_name = ':1'
GROUP BY
	ps_partkey
HAVING
	SUM(ps_supplycost * ps_availqty) > (
		SELECT
			SUM(ps_supplycost * ps_availqty) * :2
		FROM
			partsupp
		JOIN
			supplier ON ps_suppkey = s_suppkey
		JOIN
			nation ON s_nationkey = n_nationkey
		WHERE
			n_name = ':1'
	)
ORDER BY
	value DESC
