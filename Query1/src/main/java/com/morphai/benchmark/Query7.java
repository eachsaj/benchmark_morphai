package com.morphai.benchmark;

public class Query7
{
    public static void main( String[] args )
    {
        // Import Spark classes
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.builder()
            .appName("TPC-H Query7 Example")
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

        // Set the parameters for the nations
        String nation1 = "FRANCE";
        String nation2 = "GERMANY";

        // TPC-H Query 7 SQL
        String query = String.format(
            "SELECT " +
            "    supp_nation, " +
            "    cust_nation, " +
            "    l_year, " +
            "    SUM(volume) AS revenue " +
            "FROM ( " +
            "    SELECT " +
            "        n1.n_name AS supp_nation, " +
            "        n2.n_name AS cust_nation, " +
            "        YEAR(l_shipdate) AS l_year, " +
            "        l_extendedprice * (1 - l_discount) AS volume " +
            "    FROM " +
            "        supplier " +
            "        JOIN lineitem ON s_suppkey = l_suppkey " +
            "        JOIN orders ON o_orderkey = l_orderkey " +
            "        JOIN customer ON c_custkey = o_custkey " +
            "        JOIN nation n1 ON s_nationkey = n1.n_nationkey " +
            "        JOIN nation n2 ON c_nationkey = n2.n_nationkey " +
            "    WHERE " +
            "        ( " +
            "            (n1.n_name = '%s' AND n2.n_name = '%s') " +
            "            OR (n1.n_name = '%s' AND n2.n_name = '%s') " +
            "        ) " +
            "        AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' " +
            ") shipping " +
            "GROUP BY " +
            "    supp_nation, " +
            "    cust_nation, " +
            "    l_year " +
            "ORDER BY " +
            "    supp_nation, " +
            "    cust_nation, " +
            "    l_year",
            nation1, nation2, nation2, nation1
        );

        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> result = spark.sql(query);

        result.show(false); // Show all columns without truncation

        // Write result as CSV to ./output directory
        result.write().mode("overwrite").option("header", "true").csv("./output/query7_result.csv");

        spark.stop();
    }
}
