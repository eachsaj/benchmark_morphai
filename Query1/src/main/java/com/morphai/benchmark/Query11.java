package com.morphai.benchmark;

public class Query11
{
    public static void main( String[] args )
    {
        // Import Spark classes
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.builder()
            .appName("TPC-H Query11 Example")
            .master("local[*]")
            .getOrCreate();

        // Read Parquet files into DataFrames
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> partsupp = spark.read()
            .parquet("tpch_data/partsupp.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> supplier = spark.read()
            .parquet("tpch_data/supplier.parquet");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> nation = spark.read()
            .parquet("tpch_data/nation.parquet");

        // Register DataFrames as temporary views
        partsupp.createOrReplaceTempView("partsupp");
        supplier.createOrReplaceTempView("supplier");
        nation.createOrReplaceTempView("nation");

        // Set parameters
        String nationName = "GERMANY"; // Change as needed
        double fraction = 0.0001;      // Change as needed

        // TPC-H Query 11 SQL
        String query = String.format(
            "SELECT " +
            "    ps_partkey, " +
            "    SUM(ps_supplycost * ps_availqty) AS value " +
            "FROM " +
            "    partsupp " +
            "JOIN " +
            "    supplier ON ps_suppkey = s_suppkey " +
            "JOIN " +
            "    nation ON s_nationkey = n_nationkey " +
            "WHERE " +
            "    n_name = '%s' " +
            "GROUP BY " +
            "    ps_partkey " +
            "HAVING " +
            "    SUM(ps_supplycost * ps_availqty) > ( " +
            "        SELECT " +
            "            SUM(ps_supplycost * ps_availqty) * %f " +
            "        FROM " +
            "            partsupp " +
            "        JOIN " +
            "            supplier ON ps_suppkey = s_suppkey " +
            "        JOIN " +
            "            nation ON s_nationkey = n_nationkey " +
            "        WHERE " +
            "            n_name = '%s' " +
            "    ) " +
            "ORDER BY " +
            "    value DESC",
            nationName, fraction, nationName
        );

        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> result = spark.sql(query);

        result.show(false); // Show all columns without truncation

        // Write result as CSV to ./output directory
        result.write().mode("overwrite").option("header", "true").csv("./output/query11_result.csv");

        spark.stop();
    }
}
