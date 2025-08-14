/**
 * Executes TPC-H Query 1 using Apache Spark on a Parquet dataset.
 * <p>
 * This class reads the "lineitem" table from a Parquet file, performs aggregation and filtering
 * as specified in the TPC-H Query 1 benchmark, and writes the summarized results to a CSV file.
 * The query computes totals and averages for quantity, price, discount, and tax, grouped by
 * return flag and line status, for items shipped before a specified date.
 */
package com.morphai.benchmark;

public class Query1 
{
    public static void main( String[] args )
    {
        // Import Spark classes
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.builder()
            .appName("TPC-H Query1 Example")
            .master("local[*]")
            .getOrCreate();

        // Read Parquet file into DataFrame
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> df = spark.read()
            .parquet("tpch_data/lineitem.parquet");

        // Register DataFrame as a temporary view
        df.createOrReplaceTempView("lineitem");

        // Set the interval value for the date subtraction (e.g., 90 days)
        int intervalDays = 90;
        
        // Run SQL query
        String query = String.format(
            "SELECT " +
            "l_returnflag, " +
            "l_linestatus, " +
            "SUM(l_quantity) AS sum_qty, " +
            "SUM(l_extendedprice) AS sum_base_price, " +
            "SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, " +
            "SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, " +
            "AVG(l_quantity) AS avg_qty, " +
            "AVG(l_extendedprice) AS avg_price, " +
            "AVG(l_discount) AS avg_disc, " +
            "COUNT(*) AS count_order " +
            "FROM lineitem " +
            "WHERE l_shipdate <= DATE_SUB(DATE('1998-12-01'), %d) " +
            "GROUP BY l_returnflag, l_linestatus " +
            "ORDER BY l_returnflag, l_linestatus", intervalDays);

        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> result = spark.sql(query);

        result.show(false); // Show all columns without truncation

        // Write result as Parquet to ./out directory
        result.write().mode("overwrite").option("header", "true")
        .csv("./output/query1_result");

        spark.stop();
    }
}
