 # TPC-H Parquet dataset on macOS
To generate a TPC-H Parquet dataset on macOS, you have a couple of efficient options. 

One popular method is to use the `tpchgen-rs` tool. This tool is a Rust-based implementation of the TPC-H data generator, specifically designed to output data in Parquet format, which is well-suited for analytics and efficient storage. `tpchgen-rs` is advantageous because it leverages the benefits of Rust's performance and safety features while producing data in a modern columnar format.

Alternatively, you can utilize the official TPC-H data generator, known as `dbgen`. This tool generates data in a flat file format (typically CSV). To convert the generated CSV files into Parquet format, you can use a database engine like DuckDB. DuckDB is particularly useful because it can read the CSV files directly and efficiently convert them into Parquet format, taking advantage of its capabilities for analytical queries.

To get started, you can refer to the official TPC-H documentation, which provides comprehensive details on the data generation process, including the schema, data characteristics, and guidelines for using both `dbgen` and `tpchgen-rs`. This documentation will help you understand the parameters you can set during the generation process, ensuring you customize the dataset to your specific needs.

Overall, both methods are effective for generating TPC-H datasets in Parquet format, so you can choose the one that best fits your workflow and requirements.
Tool: https://github.com/clflushopt/tpchgen-rs
## Prerequisite
[Install Rust](https://www.rust-lang.org/tools/install)
## Procedure
```
cargo install tpchgen-cli
```
```
tpchgen-cli --scale-factor 1 --output-dir ./ --format parquet
```
## Generated Data Model


<img width="850" height="531" alt="image" src="https://github.com/user-attachments/assets/280de7da-dbea-44bc-b096-4412cb246bd7" />

[Image Source](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf)

# To download the generated Parquet files
Please use the [link](https://drive.google.com/drive/folders/1kkWU8VtIqB1-2Pu1u7Rg2fl2BeiE3k3F?usp=sharing) 

```