# TPC-DS benchmark with Spark #

This is a TPC-DS benchmark kit ported from [spark-sql-perf](https://github.com/databricks/spark-sql-perf "spark-sql-perf"). Table creation and query execution can be launched with `spark-submit`, and user can select specific queries to execute and benchmark.

By default supports Spark 3.0. To run with Spark 2.4+, please modify `build.sbt` with the required version.

## Generate data ##

### Download and compile tpc-ds kit ###
```bash
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX
```
### Generate test data ###
If `zoo-tutorials` is not cloned with `--recursive` option, firstly clone submodule `spark-sql-perf` into directory with
```bash
cd /path/to/zoo-tutorials
git submodule update --init
```
Then generate TPC-DS data
```bash
cd tpcds-spark/spark-sql-perf
sbt "test:runMain com.databricks.spark.sql.perf.tpcds.GenTPCDSData -d <dsdgenDir> -s <scaleFactor> -l <dataDir> -f parquet"
```
`dsdgenDir` is the path of `tpcds-kit/tools`, `scaleFactor` is the size of the data, for example `-s 1` will generate 1G data, `dataDir` is the path to store generated data.

## Run benchmark ##
Note that all below steps should be run under `zoo-tutorials/tpcds-spark` directory.
### Compile kit ###
```bash
sbt package
```
### Create external tables ###
```bash
$SPARK_HOME/bin/spark-submit \
        --class "createTables" \
        --master <spark-master> \
        --driver-memory 20G \
        --executor-cores <executor-cores> \
        --total-executor-cores <total-cores> \
        --executor-memory 20G \
        --jars spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
        target/scala-2.12/tpcds-benchmark_2.12-0.1.jar <dataDir> <dsdgenDir> <scaleFactor>
```
### Execute TPC-DS queries ###
Argument `outputDir` is the path of results, optional argument `query`  is the query number to run. Multiple query numbers should be separated by space, e.g. `1 2 3`. If no query number is specified, all 1-99 queries would be executed.
```bash
$SPARK_HOME/bin/spark-submit \
        --class "TPCDSBenchmark" \
        --master <spark-master> \
        --driver-memory 20G \
        --executor-cores <executor-cores> \
        --total-executor-cores <total-cores> \
        --executor-memory 20G \
        --jars spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
        --conf spark.speculation=false \
        --conf spark.io.compression.codec=lz4 \
        --conf spark.sql.shuffle.partitions=<partitions> \
        target/scala-2.12/tpcds-benchmark_2.12-0.1.jar <outputDir> [query]
```
After benchmark is finished, the performance result is saved as `part-*.csv` file under `<outputDir>/performance` directory.
