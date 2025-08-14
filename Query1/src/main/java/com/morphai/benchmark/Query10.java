package com.morphai.benchmark;

public class Query10 {
    public static void main(String[] args) {
        // Import Spark classes
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.builder()
                .appName("TPC-H Query10 Example")
                .master("local[*]")
                .getOrCreate();

        // Read Parquet files into DataFrames
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

        // Register DataFrames as temporary views
        lineitem.createOrReplaceTempView("lineitem");
        supplier.createOrReplaceTempView("supplier");
        orders.createOrReplaceTempView("orders");
        customer.createOrReplaceTempView("customer");
        nation.createOrReplaceTempView("nation");

        // Set the parameter for the start date
        String startDate = "1993-10-01"; // You can change this as needed

        // TPC-H Query 10 SQL
        String query = String.format(
                "SELECT " +
                        "    c_custkey, " +  "    c_name, " +
                        "    SUM(l_extendedprice * (1 - l_discount)) AS revenue, " +
                        "    c_acctbal, " + "    n_name, " +
                        "    c_address, " + "    c_phone, " +
                        "    c_comment " +  "FROM " +
                        "    customer " +   "JOIN orders ON c_custkey = o_custkey " +
                        "JOIN lineitem ON l_orderkey = o_orderkey " +
                        "JOIN nation ON c_nationkey = n_nationkey " +
                        "WHERE " + "    o_orderdate >= DATE('%s') " +
                        "    AND o_orderdate < ADD_MONTHS(DATE('%s'), 3) " +
                        "    AND l_returnflag = 'R' " +
                        "GROUP BY " +
                        "    c_custkey, " +     "    c_name, " +
                        "    c_acctbal, " +     "    c_phone, " +
                        "    n_name, " +        "    c_address, " +
                        "    c_comment " +      "ORDER BY " +
                        "    revenue DESC " +   "LIMIT 20",
                startDate, startDate);

        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> result = spark.sql(query);

        result.show(false); // Show all columns without truncation

        // Write result as CSV to ./output directory
        result.write().mode("overwrite").option("header", "true").csv("./output/query10_result.csv");

        spark.stop();
    }
}
