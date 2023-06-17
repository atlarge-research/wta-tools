package com.asml.apa.wta.spark;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.enums.Domain;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import com.asml.apa.wta.spark.streams.MetricStreamingEngine;
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

  protected SparkDataSource sut2;

  protected JavaRDD<String> textFile;

  RuntimeConfig fakeConfig;

  RuntimeConfig fakeConfig2;

  MetricStreamingEngine fakeMetricStreamingEngine;

  @BeforeEach
  void setupBaseIntegrationTest() {
    fakeConfig = RuntimeConfig.builder()
        .authors(new String[] {"Harry Potter"})
        .domain(Domain.SCIENTIFIC)
        .description("Yer a wizard harry")
        .logLevel("INFO")
        .isStageLevel(false)
        .resourcePingInterval(500)
        .executorSynchronizationInterval(-100)
        .outputPath("src/test/resources/wta-output")
        .build();

    fakeConfig2 = RuntimeConfig.builder()
        .authors(new String[] {"Harry Potter"})
        .isStageLevel(true)
        .domain(Domain.SCIENTIFIC)
        .description("Yer a wizard harry")
        .outputPath("src/test/resources/WTA")
        .build();

    SparkConf conf = new SparkConf()
        .setAppName("SparkTestRunner")
        .setMaster("local");
    spark = SparkSession.builder().config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    fakeMetricStreamingEngine = new MetricStreamingEngine();

    sut = new SparkDataSource(spark.sparkContext(), fakeConfig);
    sut2 = new SparkDataSource(spark.sparkContext(), fakeConfig2);
    String resourcePath = "src/test/resources/wordcount.txt";
    textFile = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(resourcePath);
  }

  /**
   * Need to invoke this method to call the Application end callbacks before everything ends (so that the callback is called before the assertions)
   */
  protected void stopJob() {
    spark.sparkContext().stop();
  }

  /**
   * Method to invoke the Spark operation. Important to invoke collect() to store the metrics.
   */
  protected void invokeJob() {
    textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey(Integer::sum)
        .collect();
  }
}
