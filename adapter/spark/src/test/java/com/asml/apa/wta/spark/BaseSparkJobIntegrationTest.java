package com.asml.apa.wta.spark;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.enums.Domain;
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

  RuntimeConfig fakeConfig;

  @BeforeEach
  void setupBaseIntegrationTest() {
    fakeConfig = RuntimeConfig.builder()
        .authors(new String[] {"Harry Potter"})
        .isStageLevel(true)
        .domain(Domain.SCIENTIFIC)
        .description("Yer a wizard harry")
        .outputPath("src/test/resources/WTA")
        .build();

    SparkConf conf = new SparkConf()
        .setAppName("SparkTestRunner")
        .setMaster("local[1]")
        .set("spark.executor.instances", "1") // 1 executor per instance of each worker
        .set("spark.executor.cores", "2"); // 2 cores on each executor
    spark = SparkSession.builder().config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    sut = new SparkDataSource(spark.sparkContext(), fakeConfig);
    String resourcePath = "src/test/resources/wordcount.txt";
    testFile = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(resourcePath);
  }

  /**
   * Need to invoke this method to call the Application end callbacks before everything ends (so that the callback is called before the assertions)
   */
  protected void stopJob() {
    spark.sparkContext().stop();
  }

  protected void invokeJob() {
    testFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((a, b) -> a + b)
        .collect(); // important to collect to store the metrics
  }
}
