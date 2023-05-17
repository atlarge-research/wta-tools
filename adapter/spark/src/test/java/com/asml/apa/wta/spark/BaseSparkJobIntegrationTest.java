package com.asml.apa.wta.spark;

import com.asml.apa.wta.spark.datasource.SparkDataSource;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import scala.Tuple2;

public class BaseSparkJobIntegrationTest {

  protected SparkSession spark;
  protected SparkDataSource sut;

  protected JavaRDD<String> testFile;

  @BeforeEach
  void setup() {
    SparkConf conf = new SparkConf()
        .setAppName("SparkTestRunner")
        .setMaster("local[1]")
        .set("spark.executor.instances", "1") // 1 executor per instance of each worker
        .set("spark.executor.cores", "2"); // 2 cores on each executor
    spark = SparkSession.builder().config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    sut = new SparkDataSource(spark.sparkContext());
    String resourcePath = "src/test/resources/wordcount.txt";
    testFile = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(resourcePath);
  }

  protected void invokeJob() {
    testFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((a, b) -> a + b)
        .collect(); // important to collect to store the metrics
  }
}
