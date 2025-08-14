package com.morphai.benchmark;

public class Query4
{
    public static void main( String[] args )
    {
        // Import Spark classes
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.builder()
            .appName("TPC-H Query4 Example")
            .master("local[*]")
            .getOrCreate();

        // Read Parquet files into DataFrames
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> orders = spark.read()
            .parquet("tpch_data/orders.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> lineitem = spark.read()
            .parquet("tpch_data/lineitem.parquet");

        // Register DataFrames as temporary views
        orders.createOrReplaceTempView("orders");
        lineitem.createOrReplaceTempView("lineitem");

        // Set the date parameter
        String dateParam = "1993-07-01";

        // Run SQL query
        String query = String.format(
            "SELECT " +
            "    o_orderpriority, " +
            "    COUNT(*) AS order_count " +
            "FROM " +
            "    orders " +
            "WHERE " +
            "    o_orderdate >= DATE('%s') " +
          
            "    AND EXISTS ( " +
            "        SELECT 1 " +
            "        FROM lineitem " +
            "        WHERE l_orderkey = o_orderkey " +
            "            AND l_commitdate < l_receiptdate " +
            "    ) " +
            "GROUP BY " +
            "    o_orderpriority " +
            "ORDER BY " +
            "    o_orderpriority", dateParam, dateParam);

        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> result = spark.sql(query);

        result.show(false); // Show all columns without truncation

        // Write result as CSV to ./output directory
        result.write().mode("overwrite").option("header", "true").csv("./output/query4_result.csv");

        spark.stop();
    }
}
