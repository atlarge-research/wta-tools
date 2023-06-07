# TPC-DS benchmark with Spark
This is a TPC-DS benchmark kit ported from [spark-sql-perf](https://github.com/databricks/spark-sql-perf "spark-sql-perf"). Table creation and query execution can be launched with `spark-submit`, and user can select specific queries to execute and benchmark.

Currently, this module supports Spark 3.2.4. To run with other Spark versions, please modify `build.sbt` with the required version.

To use this kit, you must download the **tpcds-kit** from **Databricks**. Please follow the instructions below to download and compile the kit.

## Requirements
Ensure that you have the following frameworks installed and path variables set:
- Apache Spark 3.2.4 (Scala 2.12.x)
- Scala 2.12.x
- sbt 0.13.x
- tpcds-kit

## Running TPC-DS benchmark

### Download and compile the TPC-DS kit
The TPC-DS tool-kit is only available to run with Ubuntu or CentOS. For other distributions, please use a virtual machine.

For ubuntu:
```bash
`sudo apt-get install gcc-9 g++-9 make flex bison byacc git`
```

For CentOS:
```bash
`sudo yum install gcc-9 g++-9 make flex bison byacc git`
```

Download and TPC-DS kit and compilt it:

```bash
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make CC=gcc-9 OS=LINUX
```

### Generate test data ##
Generate TPC-DS data:
```bash
cd tpcds-spark/spark-sql-perf
sbt "test:runMain com.databricks.spark.sql.perf.tpcds.GenTPCDSData -d <dsdgenDir> -s <scaleFactor> -l <dataDir> -f parquet"
```
`dsdgenDir` is the path of `tpcds-kit/tools`, `scaleFactor` is the size of the data, for example `-s 1` will generate 1G data, `dataDir` is the path to store generated data.

### Compile tpcds-spark kit ###
All bash commands below should be run from the `zoo-tutorials/tpcds-spark` directory.

Enter the following command to compile the kit and package it:
```bash
sbt package
```

### Create external tables ###
Create tables in Spark:

```bash
spark-submit \
--class "createTables" \
--master <spark-master> \
--driver-memory <driver memory> \
--executor-cores <executor-cores> \
--total-executor-cores <total-cores> \
--executor-memory <executor memory> \
--jars spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
target/scala-2.12/tpcds-benchmark_2.12-0.1.jar <dataDir> <dsdgenDir> <scaleFactor>
```

`dataDir` is the path to store generated data, `dsdgenDir` is the path of `tpcds-kit/tools`, `scaleFactor` is the size of the generated data.

### Execute TPC-DS queries ###
Execute the following queries to run TPC-DS benchmark.

```bash
spark-submit \
--class "TPCDSBenchmark" \
--master <spark-master> \
--driver-memory <driver memory> \
--executor-cores <executor-cores> \
--total-executor-cores <total-cores> \
--executor-memory <executor memory> \
--jars spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
--conf spark.speculation=false \
--conf spark.io.compression.codec=lz4 \
--conf spark.sql.shuffle.partitions=<partitions> \
target/scala-2.12/tpcds-benchmark_2.12-0.1.jar <outputDir> [query]
```

`outputDir` is the path of results, optional argument `query`  is the query number to run. Multiple query numbers should be separated by space, e.g. `1 2 3`. If no query number is specified, all 1-99 queries would be executed.

After benchmark is finished, the performance result is saved as `part-*.csv` file under `<outputDir>/performance` directory.
