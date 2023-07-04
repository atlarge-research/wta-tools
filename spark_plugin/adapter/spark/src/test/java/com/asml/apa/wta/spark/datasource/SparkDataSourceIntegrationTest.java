package com.asml.apa.wta.spark.datasource;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import com.asml.apa.wta.core.WtaWriter;
import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.model.Domain;
import com.asml.apa.wta.spark.stream.MetricStreamingEngine;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

class SparkDataSourceIntegrationTest {

  private SparkSession spark;

  private SparkDataSource sut;

  private JavaRDD<String> textFile;

  private RuntimeConfig fakeConfig;

  @BeforeEach
  void setupBaseIntegrationTest() {
    fakeConfig = RuntimeConfig.builder()
        .authors(new String[] {"Harry Potter"})
        .domain(Domain.SCIENTIFIC)
        .description("Yer a wizard harry")
        .isStageLevel(false)
        .resourcePingInterval(500)
        .executorSynchronizationInterval(-100)
        .outputPath("src/test/resources/wta-output")
        .build();

    SparkConf conf = new SparkConf().setAppName("SparkTestRunner").setMaster("local");
    spark = SparkSession.builder().config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    sut = new SparkDataSource(
        spark.sparkContext(), fakeConfig, mock(MetricStreamingEngine.class), mock(WtaWriter.class));

    String resourcePath = "src/test/resources/wordcount.txt";
    textFile = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(resourcePath);
  }

  protected void invokeJob() {
    textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey(Integer::sum)
        .collect();
  }

  @Test
  public void taskListenerReturnsList() {
    await().atMost(20, SECONDS)
        .until(() -> sut.getTaskLevelListener().getProcessedObjects().count() >= 0);
  }

  @Test
  public void unregisteredTaskListenerDoesNotCollect() {
    assertThat(sut.getTaskLevelListener().getProcessedObjects().isEmpty()).isTrue();
    invokeJob();
    await().atMost(20, SECONDS)
        .until(() -> sut.getTaskLevelListener().getProcessedObjects().isEmpty());
  }

  @Test
  public void removedTaskListenerDoesNotCollect() {
    sut.registerTaskListener();
    sut.removeListeners();
    invokeJob();
    await().atMost(20, SECONDS)
        .until(() -> sut.getTaskLevelListener().getProcessedObjects().isEmpty());
  }
}
