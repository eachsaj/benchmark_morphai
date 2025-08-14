
package com.morphai.benchmark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class Query3 
{
    public static void main( String[] args )
    {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
            .appName("TPC-H Query3 Example")
            .master("local[*]")
            .getOrCreate();

        // Read Parquet files into DataFrames
        Dataset<Row> customer = spark.read()
            .parquet("tpch_data/customer.parquet");
        Dataset<Row> orders = spark.read()
            .parquet("tpch_data/orders.parquet");
        Dataset<Row> lineitem = spark.read()
            .parquet("tpch_data/lineitem.parquet");

        // Register DataFrames as temporary views
        customer.createOrReplaceTempView("customer");
        orders.createOrReplaceTempView("orders");
        lineitem.createOrReplaceTempView("lineitem");

        // Set parameters
        String mktsegment = "BUILDING"; // replace with desired segment
        String date = "1995-03-15";     // replace with desired date

        // Run SQL query
        String query = String.format(
            "SELECT " +
            "    l.l_orderkey, " +
            "    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, " +
            "    o.o_orderdate, " +
            "    o.o_shippriority " +
            "FROM " +
            "    customer c " +
            "    JOIN orders o ON c.c_custkey = o.o_custkey " +
            "    JOIN lineitem l ON l.l_orderkey = o.o_orderkey " +
            "WHERE " +
            "    c.c_mktsegment = '%s' " +
            "    AND o.o_orderdate < DATE '%s' " +
            "    AND l.l_shipdate > DATE '%s' " +
            "GROUP BY " +
            "    l.l_orderkey, " +
            "    o.o_orderdate, " +
            "    o.o_shippriority " +
            "ORDER BY " +
            "    revenue DESC, " +
            "    o.o_orderdate " +
            "LIMIT 10", mktsegment, date, date);

        Dataset<Row> result = spark.sql(query);

        result.show(false); // Show all columns without truncation

        // Write result as CSV to ./output directory
        result.write().mode("overwrite").option("header", "true").csv("./output/query3_result");

        spark.stop();
    }
}
