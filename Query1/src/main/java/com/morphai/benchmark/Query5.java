package com.morphai.benchmark;

public class Query5 
{
    public static void main( String[] args )
    {
        // Import Spark classes
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.builder()
            .appName("TPC-H Query5 Example")
            .master("local[*]")
            .getOrCreate();

        // Read Parquet files into DataFrames
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> customer = spark.read()
            .parquet("tpch_data/customer.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> orders = spark.read()
            .parquet("tpch_data/orders.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> lineitem = spark.read()
            .parquet("tpch_data/lineitem.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> supplier = spark.read()
            .parquet("tpch_data/supplier.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> nation = spark.read()
            .parquet("tpch_data/nation.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> region = spark.read()
            .parquet("tpch_data/region.parquet");

        // Register DataFrames as temporary views
        customer.createOrReplaceTempView("customer");
        orders.createOrReplaceTempView("orders");
        lineitem.createOrReplaceTempView("lineitem");
        supplier.createOrReplaceTempView("supplier");
        nation.createOrReplaceTempView("nation");
        region.createOrReplaceTempView("region");

        // Set the parameters
        String regionName = "ASIA";
        String startDate = "1994-01-01";

        // Run SQL query
        String query = String.format(
            "SELECT " +
            "    n_name, " +
            "    SUM(l_extendedprice * (1 - l_discount)) AS revenue " +
            "FROM " +
            "    customer " +
            "JOIN orders ON c_custkey = o_custkey " +
            "JOIN lineitem ON l_orderkey = o_orderkey " +
            "JOIN supplier ON l_suppkey = s_suppkey " +
            "JOIN nation ON s_nationkey = n_nationkey AND c_nationkey = s_nationkey " +
            "JOIN region ON n_regionkey = r_regionkey " +
            "WHERE " +
            "    r_name = '%s' " +
            "    AND o_orderdate >= DATE('%s') " +
            "    AND o_orderdate < DATE_ADD(DATE('%s'), 365) " +
            "GROUP BY " +
            "    n_name " +
            "ORDER BY " +
            "    revenue DESC",
            regionName, startDate, startDate);

        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> result = spark.sql(query);

        result.show(false); // Show all columns without truncation

        // Write result as CSV to ./output directory
        result.write().mode("overwrite").option("header", "true").csv("./output/query5_result.csv");

        spark.stop();
    }
}
