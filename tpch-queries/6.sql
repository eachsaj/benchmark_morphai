-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6) - Spark SQL Compatible

SELECT
	SUM(l_extendedprice * l_discount) AS revenue
FROM
	lineitem
WHERE
	l_shipdate >= DATE('{start_date}')
	AND l_shipdate < DATE_ADD(DATE('{start_date}'), 365)
	AND l_discount BETWEEN {discount} - 0.01 AND {discount} + 0.01
	AND l_quantity < {quantity};
