# Benchmarking
This directory provides all the tools we use for benchmarking the Spark plugin. These tools are independent of the Spark plugin itself, and can be used to benchmark any Spark application. Furthermore, these will *not* be packaged with the Spark plugin.

## Directory structure
This directory contains the following tools/module or instructions to download them:
- **das-bigdata-deployment**
- **zoo-tutorials** 
- **tpcds-kit**

## DAS-5
DAS-5 or Distributed ASCII Supercomputer 5 (DAS-5) is a cluster of distributed high-end servers. This will be the production environment setting on which Spark and the Spark plugin will be running.

Supplementary information can be found [on the ASCI website](https://asci.tudelft.nl/project-das/).

### das-bigdata-deployment
This is the script to download, configure, and deploy Hadoop HDFS, YARN, and Spark on the Distributed ASCII Supercomputer (DAS-5). The DAS scripts are Python scripts that submit a slot allocation request on the DAS, whilst configuring the Spark environment. It is based on [the work of Tim Hegeman, Chris Lemaire, and Laurens Versluis](https://github.com/lfdversluis/das-bigdata-deployment). You can find the forked source repo [here](https://github.com/lfdversluis/das-bigdata-deployment).

For more detailed information on how to use this script, go to the [README](benchmarking/das-bigdata-deployment/README.md) in the **das-bigdata-deployment** directory.

## Running TPC-DS benchmark
The instructions below are meant for running the TPC-DS benchmark locally. In order to run it on DAS-5, make use of the **das-bigdata-deployment** module and modify accordingly.

### Requirements
Ensure that you have the following tools installed and environment path variables set:
- Apache Spark 3.2.4 (Scala 2.12.x)
- Java 11
- Scala 2.12.x
- sbt 0.13.x

### zoo-tutorials
This module contains the code for running TPC-DS benchmarks on Spark. To run TPC-DS benchmarks on Spark, we only need the **tpcds-spark** submodule within **zoo-tutorials**, but it is necessary to clone the entire repo in order to properly package it using sbt.

Download the repository:
```shell
git clone https://github.com/intel-analytics/zoo-tutorials.git
```

Clone submodule spark-sql-perf into directory:
```shell
cd /path/to/zoo-tutorials
git submodule update --init
```

Currently, this module supports Spark 3.0.0. To run with other Spark versions, please modify `build.sbt` in **zoo-tutorials/tpcds-spark**  with the required version (e.g. Spark 3.2.4)

Enter the following command to compile **tpcds-spark** and package it:
```shell
cd zoo-tutorials/tpcds-spark
sbt package
```

### tpcds-toolkit
**tpcds-toolkit** is only available on Ubuntu or CentOS. For other distributions, please use a virtual machine.

For Ubuntu:
```shell
`sudo apt-get install gcc make flex bison byacc git`
```

For CentOS:
```shell
`sudo yum install gcc make flex bison byacc git`
```

Download and compile **tpcds-toolkit**:
```shell
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX
```

### Generate test data ##
Now that every thing is installed, generate TPC-DS data:
```shell
cd zoo-tutorials/tpcds-spark/spark-sql-perf
sbt "test:runMain com.databricks.spark.sql.perf.tpcds.GenTPCDSData -d <dsdgenDir> -s <scaleFactor> -l <dataDir> -f parquet"
```
`dsdgenDir` is the path of `tpcds-kit/tools`, `scaleFactor` is the size of the data, for example `-s 1` will generate 1G data, `dataDir` is the path to store generated data.

### Create external tables ###
Create tables in the **tpcds-spark** using `spark-submit`:

```shell
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
Execute the following queries to run TPC-DS benchmark:

```shell
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
