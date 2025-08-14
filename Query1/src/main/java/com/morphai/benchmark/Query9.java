package com.morphai.benchmark;

public class Query9 {
        public static void main(String[] args) {
                // Import Spark classes
                org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.builder()
                                .appName("TPC-H Query9 Example")
                                .master("local[*]")
                                .getOrCreate();

                // Read Parquet files into DataFrames
                org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> part = spark.read()
                                .parquet("tpch_data/part.parquet");
                org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> partsupp = spark.read()
                                .parquet("tpch_data/partsupp.parquet");
                org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> lineitem = spark.read()
                                .parquet("tpch_data/lineitem.parquet");
                org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> supplier = spark.read()
                                .parquet("tpch_data/supplier.parquet");
                org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> orders = spark.read()
                                .parquet("tpch_data/orders.parquet");
                org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> nation = spark.read()
                                .parquet("tpch_data/nation.parquet");

                // Register DataFrames as temporary views
                part.createOrReplaceTempView("part");
                partsupp.createOrReplaceTempView("partsupp");
                lineitem.createOrReplaceTempView("lineitem");
                supplier.createOrReplaceTempView("supplier");
                orders.createOrReplaceTempView("orders");
                nation.createOrReplaceTempView("nation");

                // Set the parameter for part name pattern
                String partNamePattern = "%:1%";

                // TPC-H Query 9 SQL
                String query = String.format(
                                "SELECT " +
                                "    nation, " +
                                "    o_year, " +
                                "    SUM(amount) AS sum_profit " +
                                "FROM ( " +
                                "    SELECT " +
                                "        n_name AS nation, " +
                                "        YEAR(o_orderdate) AS o_year, " +
                                "        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount "
                                +
                                "    FROM " +
                                "        part " +
                                "        JOIN partsupp ON part.p_partkey = partsupp.ps_partkey " +
                                "        JOIN lineitem ON lineitem.l_partkey = part.p_partkey " +
                                "        AND lineitem.l_suppkey = partsupp.ps_suppkey " +
                                "        JOIN supplier ON supplier.s_suppkey = lineitem.l_suppkey " +
                                "        JOIN orders ON orders.o_orderkey = lineitem.l_orderkey " +
                                "        JOIN nation ON supplier.s_nationkey = nation.n_nationkey " +
                                "    WHERE " +
                                "        part.p_size > 5 " +
                                ") profit " +
                                "GROUP BY " +
                                "    nation, " +
                                "    o_year " +
                                "ORDER BY " +
                                "    nation, " +
                                "    o_year DESC",
                                partNamePattern);

                org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> result = spark.sql(query);

                result.show(false); // Show all columns without truncation

                // Write result as CSV to ./output directory
                result.write().mode("overwrite").option("header", "true").csv("./output/query9_result.csv");

                spark.stop();
        }
}
