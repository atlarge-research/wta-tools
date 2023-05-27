package com.asml.apa.wta.benchmarking.spark;

import com.asml.apa.wta.core.utils.WtaUtils;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarking script for the Spark plugin.
 *
 * @since 1.0.0
 * @author Pil Kyu Cho
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 50)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SparkBenchmark {

  private SparkSession spark;
  private static String resourcesPath = "benchmarking/spark-benchmarking/src/main/resources/";

  private static String tpcdsDataPath = resourcesPath + "tpcds_parquet_data/";

  private static String sqlQueryPath = resourcesPath + "sql_files/";

  private static String configFilePath = resourcesPath + "config.json";

  /**
   * Utility method to get the query String from SQL file.
   *
   * @param filepath      Filepath of the SQL file
   * @return              Query String
   * @throws IOException  throws IOException
   */
  private static String getQuery(String filepath) throws IOException {
    return Files.lines(Paths.get(filepath), StandardCharsets.UTF_8)
            .filter(line -> !line.startsWith("--")) // exclude SQL comments
            .filter(line -> !line.isBlank()) // exclude empty lines
            .map(String::trim)
            .collect(Collectors.joining(" "));
  }

  /**
   * Reads each dataset parquet file and compile them into a single dataframe of tables.
   *
   * @param spark   SparkSession instance
   */
  private static void importData(
          SparkSession spark,
          String parquetFilepath1,
          String parquetFilepath2,
          String parquetFilepath3,
          String parquetFilepath4,
          String parquetFilepath5,
          String parquetFilepath6,
          String parquetFilepath7,
          String parquetFilepath8,
          String parquetFilepath9,
          String parquetFilepath10,
          String parquetFilepath11,
          String parquetFilepath12,
          String parquetFilepath13,
          String parquetFilepath14,
          String parquetFilepath15,
          String parquetFilepath16,
          String parquetFilepath17,
          String parquetFilepath18,
          String parquetFilepath19,
          String parquetFilepath20,
          String parquetFilepath21,
          String parquetFilepath22,
          String parquetFilepath23,
          String parquetFilepath24) {
    Dataset<Row> df1 = spark.read().parquet(tpcdsDataPath + parquetFilepath1);
    Dataset<Row> df2 = spark.read().parquet(tpcdsDataPath + parquetFilepath2);
    Dataset<Row> df3 = spark.read().parquet(tpcdsDataPath + parquetFilepath3);
    Dataset<Row> df4 = spark.read().parquet(tpcdsDataPath + parquetFilepath4);
    Dataset<Row> df5 = spark.read().parquet(tpcdsDataPath + parquetFilepath5);
    Dataset<Row> df6 = spark.read().parquet(tpcdsDataPath + parquetFilepath6);
    Dataset<Row> df7 = spark.read().parquet(tpcdsDataPath + parquetFilepath7);
    Dataset<Row> df8 = spark.read().parquet(tpcdsDataPath + parquetFilepath8);
    Dataset<Row> df9 = spark.read().parquet(tpcdsDataPath + parquetFilepath9);
    Dataset<Row> df10 = spark.read().parquet(tpcdsDataPath + parquetFilepath10);
    Dataset<Row> df11 = spark.read().parquet(tpcdsDataPath + parquetFilepath11);
    Dataset<Row> df12 = spark.read().parquet(tpcdsDataPath + parquetFilepath12);
    Dataset<Row> df13 = spark.read().parquet(tpcdsDataPath + parquetFilepath13);
    Dataset<Row> df14 = spark.read().parquet(tpcdsDataPath + parquetFilepath14);
    Dataset<Row> df15 = spark.read().parquet(tpcdsDataPath + parquetFilepath15);
    Dataset<Row> df16 = spark.read().parquet(tpcdsDataPath + parquetFilepath16);
    Dataset<Row> df17 = spark.read().parquet(tpcdsDataPath + parquetFilepath17);
    Dataset<Row> df18 = spark.read().parquet(tpcdsDataPath + parquetFilepath18);
    Dataset<Row> df19 = spark.read().parquet(tpcdsDataPath + parquetFilepath19);
    Dataset<Row> df20 = spark.read().parquet(tpcdsDataPath + parquetFilepath20);
    Dataset<Row> df21 = spark.read().parquet(tpcdsDataPath + parquetFilepath21);
    Dataset<Row> df22 = spark.read().parquet(tpcdsDataPath + parquetFilepath22);
    Dataset<Row> df23 = spark.read().parquet(tpcdsDataPath + parquetFilepath23);
    Dataset<Row> df24 = spark.read().parquet(tpcdsDataPath + parquetFilepath24);

    df1.createOrReplaceTempView("call_center");
    df2.createOrReplaceTempView("catalog_page");
    df3.createOrReplaceTempView("catalog_returns");
    df4.createOrReplaceTempView("catalog_sales");
    df5.createOrReplaceTempView("customer");
    df6.createOrReplaceTempView("customer_address");
    df7.createOrReplaceTempView("customer_demographics");
    df8.createOrReplaceTempView("date_dim");
    df9.createOrReplaceTempView("household_demographics");
    df10.createOrReplaceTempView("income_band");
    df11.createOrReplaceTempView("inventory");
    df12.createOrReplaceTempView("item");
    df13.createOrReplaceTempView("promotion");
    df14.createOrReplaceTempView("reason");
    df15.createOrReplaceTempView("ship_mode");
    df16.createOrReplaceTempView("store");
    df17.createOrReplaceTempView("store_returns");
    df18.createOrReplaceTempView("store_sales");
    df19.createOrReplaceTempView("time_dim");
    df20.createOrReplaceTempView("warehouse");
    df21.createOrReplaceTempView("web_page");
    df22.createOrReplaceTempView("web_returns");
    df23.createOrReplaceTempView("web_sales");
    df24.createOrReplaceTempView("web_site");
  }

  /**
   * Setup method to initialize SparkSession. Not included in the benchmark
   * runtime measurements.
   */
  @Setup
  public void before() {
    SparkConf conf = new SparkConf()
            .setAppName("SparkPlugin")
            .setMaster("local[1]")
            // 2 executor per instance of each worker
            .set("spark.executor.instances", "2")
            // 2 cores on each executor
            .set("spark.executor.cores", "2");

    spark = SparkSession.builder().config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    importData(
            spark,
            "call_center/call_center.parquet",
            "catalog_page/catalog_page.parquet",
            "catalog_returns/catalog_returns.parquet",
            "catalog_sales/catalog_sales.parquet",
            "customer/customer.parquet",
            "customer_address/customer_address.parquet",
            "customer_demographics/customer_demographics.parquet",
            "date_dim/date_dim.parquet",
            "household_demographics/household_demographics.parquet",
            "income_band/income_band.parquet",
            "inventory/inventory.parquet",
            "item/item.parquet",
            "promotion/promotion.parquet",
            "reason/reason.parquet",
            "ship_mode/ship_mode.parquet",
            "store/store.parquet",
            "store_returns/store_returns.parquet",
            "store_sales/store_sales.parquet",
            "time_dim/time_dim.parquet",
            "warehouse/warehouse.parquet",
            "web_page/web_page.parquet",
            "web_returns/web_returns.parquet",
            "web_sales/web_sales.parquet",
            "web_site/web_site.parquet");
  }

  /**
   * Shuts down the Spark session after every iteration it was invoked.
   */
  @TearDown
  public void after() {
    spark.stop();
  }

  /**
   * Spark job with no plugin that runs all queries from 1 to 99. This is used as a
   * baseline to compare with Spark job with the plugin.
   *
   * @param blackhole consumes the result of the query to avoid compiler optimization
   */
  @Benchmark
  public void coreRunAllQueries(Blackhole blackhole) {
    String query;
    for (int i = 1; i < 100; i++) {
      try {
        query = getQuery(sqlQueryPath + "query" + i + ".sql");
        blackhole.consume(spark.sql(query).showString(1, 0, false));
      } catch (Exception e) {
      }
    }
  }

  /**
   * Spark job that runs all queries from 1 to 99 with the plugin enabled.
   *
   * @param blackhole consumes the result of the query to avoid compiler optimization
   */
  @Benchmark
  public void pluginRunAllQueries(Blackhole blackhole) {
    SparkDataSource sut = new SparkDataSource(spark.sparkContext(), WtaUtils.readConfig(configFilePath));
    sut.registerTaskListener();
    sut.registerJobListener();
    sut.registerApplicationListener();
    String query;
    for (int i = 1; i < 100; i++) {
      try {
        query = getQuery(sqlQueryPath + "query" + i + ".sql");
        blackhole.consume(spark.sql(query).showString(1, 0, false));
      } catch (Exception e) {
      }
    }
  }
}
