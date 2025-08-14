SELECT
	o_orderpriority,
	COUNT(*) AS order_count
FROM
	orders
WHERE
	o_orderdate >= DATE(':1')
	AND o_orderdate < ADD_MONTHS(DATE(':1'), 3)
	AND EXISTS (
		SELECT 1
		FROM lineitem
		WHERE l_orderkey = o_orderkey
			AND l_commitdate < l_receiptdate
	)
GROUP BY
	o_orderpriority
ORDER BY
	o_orderpriority
