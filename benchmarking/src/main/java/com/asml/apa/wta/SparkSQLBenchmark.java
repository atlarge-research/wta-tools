package com.asml.apa.wta;

import ch.cern.sparkmeasure.TaskMetrics;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Warmup(iterations = 20)
@Measurement(iterations = 50)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SparkSQLBenchmark {

    private SparkSession spark;
    private TaskMetrics taskMetrics;
    private String baseQueryFilePath = "benchmarking/src/main/resources/sql_files/query";

    /**
     * utility method to get the query String from SQL file
     *
     * @param filepath  Filepath of the SQL file
     * @return          Query String
     * @throws IOException
     */
    private static String getSQLQuery(String filepath) throws IOException {
        return Files.lines(Paths.get(filepath), StandardCharsets.UTF_8)
                .filter(line -> !line.startsWith("--")) // exclude SQL comments
                .filter(line -> !line.isBlank()) // exclude empty lines
                .map(String::trim)
                .collect(Collectors.joining(" "));
    }

    /**
     * Reads each dataset parquet file and compile them into a single dataframe of tables
     *
     * @param spark                 SparkSession instances
     */
    private static void exportTCPDSData(
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
            String parquetFilepath24
    ) {
        Dataset<Row> df1 = spark.read().parquet(parquetFilepath1);
        Dataset<Row> df2 = spark.read().parquet(parquetFilepath2);
        Dataset<Row> df3 = spark.read().parquet(parquetFilepath3);
        Dataset<Row> df4 = spark.read().parquet(parquetFilepath4);
        Dataset<Row> df5 = spark.read().parquet(parquetFilepath5);
        Dataset<Row> df6 = spark.read().parquet(parquetFilepath6);
        Dataset<Row> df7 = spark.read().parquet(parquetFilepath7);
        Dataset<Row> df8 = spark.read().parquet(parquetFilepath8);
        Dataset<Row> df9 = spark.read().parquet(parquetFilepath9);
        Dataset<Row> df10 = spark.read().parquet(parquetFilepath10);
        Dataset<Row> df11 = spark.read().parquet(parquetFilepath11);
        Dataset<Row> df12 = spark.read().parquet(parquetFilepath12);
        Dataset<Row> df13 = spark.read().parquet(parquetFilepath13);
        Dataset<Row> df14 = spark.read().parquet(parquetFilepath14);
        Dataset<Row> df15 = spark.read().parquet(parquetFilepath15);
        Dataset<Row> df16 = spark.read().parquet(parquetFilepath16);
        Dataset<Row> df17 = spark.read().parquet(parquetFilepath17);
        Dataset<Row> df18 = spark.read().parquet(parquetFilepath18);
        Dataset<Row> df19 = spark.read().parquet(parquetFilepath19);
        Dataset<Row> df20 = spark.read().parquet(parquetFilepath20);
        Dataset<Row> df21 = spark.read().parquet(parquetFilepath21);
        Dataset<Row> df22 = spark.read().parquet(parquetFilepath22);
        Dataset<Row> df23 = spark.read().parquet(parquetFilepath23);
        Dataset<Row> df24 = spark.read().parquet(parquetFilepath24);

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
     * Setup method to initialize SparkSession and TaskMetrics. Not included in the benchmark
     * runtime measurements.
     */
    @Setup
    public void setup() {
        System.out.println("Setting up Spark session");

        SparkConf conf = new SparkConf()
                .setAppName("SparkSQLBenchmark")
                .setMaster("local[1]")
                // 1 executor per instance of each worker
                .set("spark.executor.instances", "1")
                // 4 cores on each executor
                .set("spark.executor.cores", "4");
        spark = SparkSession.builder().config(conf).getOrCreate();
        taskMetrics = new TaskMetrics(spark);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        String resourcesPath = "benchmarking/src/main/resources/tpcds_data/";
        exportTCPDSData(
                spark,
                resourcesPath + "call_center/part-00000-52733cca-0604-4ebb-ba8f-612f9aa375ab-c000.snappy.parquet",
                resourcesPath + "catalog_page/part-00000-4b931ee0-00f2-4b2d-958e-fbdca8b1ed70-c000.snappy.parquet",
                resourcesPath + "catalog_returns/part-00000-861a1bf0-b23a-4b25-ad8c-45723ac46a5e-c000.snappy.parquet",
                resourcesPath + "catalog_sales/part-00000-a3c1f01b-fcd7-4187-9ada-ed9163fcf423-c000.snappy.parquet",
                resourcesPath + "customer/part-00000-b969e874-e1c4-42cc-9ca7-b50b7089ce62-c000.snappy.parquet",
                resourcesPath + "customer_address/part-00000-2c17ef16-76e4-424e-abb4-0fe79dbbb86e-c000.snappy.parquet",
                resourcesPath + "customer_demographics/part-00000-530769f4-7165-4749-9de1-e8115a83c77b-c000.snappy.parquet",
                resourcesPath + "date_dim/part-00000-7c16a98a-ad3d-49ac-823f-27bfc23a121a-c000.snappy.parquet",
                resourcesPath + "household_demographics/part-00000-9c7cb580-fc86-45cd-a931-4c3a57223a40-c000.snappy.parquet",
                resourcesPath + "income_band/part-00000-9fa1fc84-dfa6-44bc-92e0-72e4c41cd588-c000.snappy.parquet",
                resourcesPath + "inventory/part-00000-72d8a3d7-1c35-41ac-b5dc-86880187081c-c000.snappy.parquet",
                resourcesPath + "item/part-00000-b7cd73d1-96ce-44f5-9184-6c096c6d26d3-c000.snappy.parquet",
                resourcesPath + "promotion/part-00000-baeeb226-e42e-4cf5-91ba-a4169d65eb41-c000.snappy.parquet",
                resourcesPath + "reason/part-00000-a055c64e-9f44-41bd-ae2a-c0aa4087e1d2-c000.snappy.parquet",
                resourcesPath + "ship_mode/part-00000-64e38172-8c78-4ffb-b7ff-cb1471a1a9a5-c000.snappy.parquet",
                resourcesPath + "store/part-00000-1d5e42f9-92dc-4981-8d8c-5ffe7078abea-c000.snappy.parquet",
                resourcesPath + "store_returns/part-00000-2c234578-4908-4779-8363-6c2a95160add-c000.snappy.parquet",
                resourcesPath + "store_sales/part-00000-771c4203-7b85-46c9-90e0-832cd6f94ea8-c000.snappy.parquet",
                resourcesPath + "time_dim/part-00000-339eb101-10d8-44d6-8d5f-9617c3207194-c000.snappy.parquet",
                resourcesPath + "warehouse/part-00000-2b78937c-167a-4d13-8e8e-1b0581f190cf-c000.snappy.parquet",
                resourcesPath + "web_page/part-00000-b1d5c0cf-1a52-4104-856d-0276e928be4d-c000.snappy.parquet",
                resourcesPath + "web_returns/part-00000-7abdd026-65b2-4fad-876e-d5c7de52ead4-c000.snappy.parquet",
                resourcesPath + "web_sales/part-00000-25be34eb-a4e9-4150-880a-a138a493b76a-c000.snappy.parquet",
                resourcesPath + "web_site/part-00000-66725743-da8b-4a38-8205-0531d816b109-c000.snappy.parquet"
        );
    }

    /**
     * Teardown method that shuts down the Spark session after every iteration
     */
    @TearDown
    public void tearDown() {
        System.out.println("Tearing down Spark session");
        spark.stop();
    }

    /**
     * Plain Spark job that runs all queries from 1 to 99. This is used as a baseline to compare
     *
     * @param blackhole Blackhole to consume the result of the query
     */
    @Benchmark
    public void coreRunAllQueries(Blackhole blackhole) {
        for (int i = 1; i<100; i++) {
            try {
                String query = getSQLQuery(baseQueryFilePath +  i + ".sql");
                blackhole.consume(spark.sql(query).showString(1, 0, false));
            } catch (Exception e) {
                System.out.println("Query " + i + " failed");
            }
        }
    }

    /**
     * Spark job that runs all queries from 1 to 99 with the plugin enabled. This is used to compare
     * the performance of the plugin against the baseline.
     *
     * @param blackhole Blackhole to consume the result of the query
     */
    @Benchmark
    public void pluginRunAllQueries(Blackhole blackhole) {
        for (int i = 1; i<100; i++) {
            try {
                String query = getSQLQuery(baseQueryFilePath +  i + ".sql");
                blackhole.consume(
                        taskMetrics.runAndMeasure(() -> spark.sql(query).showString(1, 0, false))
                );
            } catch (Exception e) {
                System.out.println("Query " + i + " failed");
            }
        }
    }
}
