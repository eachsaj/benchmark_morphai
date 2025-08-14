package com.morphai.benchmark;

public class Query8
{
    public static void main( String[] args )
    {
        // Import Spark classes
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.builder()
            .appName("TPC-H Query8 Example")
            .master("local[*]")
            .getOrCreate();

        // Read Parquet files into DataFrames
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> part = spark.read()
            .parquet("tpch_data/part.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> lineitem = spark.read()
            .parquet("tpch_data/lineitem.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> supplier = spark.read()
            .parquet("tpch_data/supplier.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> orders = spark.read()
            .parquet("tpch_data/orders.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> customer = spark.read()
            .parquet("tpch_data/customer.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> nation = spark.read()
            .parquet("tpch_data/nation.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> region = spark.read()
            .parquet("tpch_data/region.parquet");

        // Register DataFrames as temporary views
        part.createOrReplaceTempView("part");
        lineitem.createOrReplaceTempView("lineitem");
        supplier.createOrReplaceTempView("supplier");
        orders.createOrReplaceTempView("orders");
        customer.createOrReplaceTempView("customer");
        nation.createOrReplaceTempView("nation");
        region.createOrReplaceTempView("region");

        // Set the parameters for the query
        String nationParam = "GERMANY";
        String regionParam = "EUROPE";
        String typeParam = "ECONOMY ANODIZED STEEL";

        // TPC-H Query 8 SQL
        String query = String.format(
            "SELECT " +
            "    o_year, " +
            "    SUM(CASE WHEN nation = '%s' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share " +
            "FROM ( " +
            "    SELECT " +
            "        YEAR(o_orderdate) AS o_year, " +
            "        l_extendedprice * (1 - l_discount) AS volume, " +
            "        n2.n_name AS nation " +
            "    FROM " +
            "        part " +
            "        JOIN lineitem ON p_partkey = l_partkey " +
            "        JOIN supplier ON s_suppkey = l_suppkey " +
            "        JOIN orders ON l_orderkey = o_orderkey " +
            "        JOIN customer ON o_custkey = c_custkey " +
            "        JOIN nation n1 ON c_nationkey = n1.n_nationkey " +
            "        JOIN region ON n1.n_regionkey = r_regionkey " +
            "        JOIN nation n2 ON s_nationkey = n2.n_nationkey " +
            "    WHERE " +
            "        r_name = '%s' " +
            "        AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' " +
            "        AND p_type = '%s' " +
            ") all_nations " +
            "GROUP BY o_year " +
            "ORDER BY o_year",
            nationParam, regionParam, typeParam
        );

        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> result = spark.sql(query);

        result.show(false); // Show all columns without truncation

        // Write result as CSV to ./output directory
        result.write().mode("overwrite").option("header", "true").csv("./output/query8_result.csv");

        spark.stop();
    }
}
