package com.morphai.benchmark;

public class Query6
{
    public static void main( String[] args )
    {
        // Import Spark classes
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.builder()
            .appName("TPC-H Query6 Example")
            .master("local[*]")
            .getOrCreate();

        // Read Parquet file into DataFrame
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> lineitem = spark.read()
            .parquet("tpch_data/lineitem.parquet");

        // Register DataFrame as temporary view
        lineitem.createOrReplaceTempView("lineitem");

        // Set the parameters
        String startDate = "1994-01-01";
        double discount = 0.06;
        int quantity = 24;

        // Run SQL query for TPC-H Q6
        String query = String.format(
            "SELECT " +
            "    SUM(l_extendedprice * l_discount) AS revenue " +
            "FROM " +
            "    lineitem " +
            "WHERE " +
            "    l_shipdate >= DATE('%s') " +
            "    AND l_shipdate < DATE_ADD(DATE('%s'), 365) " +
            "    AND l_discount BETWEEN %.2f - 0.01 AND %.2f + 0.01 " +
            "    AND l_quantity < %d",
            startDate, startDate, discount, discount, quantity);

        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> result = spark.sql(query);

        result.show(false); // Show all columns without truncation

        // Write result as CSV to ./output directory
        result.write().mode("overwrite").option("header", "true").csv("./output/query6_result.csv");

        spark.stop();
    }
}
