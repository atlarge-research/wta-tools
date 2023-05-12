package com.asml.apa.wta.spark;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.datasource.SparkDataSource;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

public class SparkDataSourceTest {

  private SparkSession spark;
  private SparkDataSource sut;

  private JavaRDD<String> testFile;

  @BeforeEach
  public void setup() {
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

  private void invokeJob() {
    testFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((a, b) -> a + b)
        .collect(); // important to collect to store the metrics
  }

  @Test
  public void sparkDatasourceIsNotNull() {
    assertThat(sut).isNotNull();
  }

  @Test
  public void taskListenerReturnsList() {
    assertThat(sut.getTaskMetrics()).isEmpty();
  }

  @Test
  public void registeredTaskListenerCollectsMetrics() {
    sut.registerTaskListener();
    assertThat(sut.getTaskMetrics()).isEmpty();
    invokeJob();
    assertThat(sut.getTaskMetrics()).isNotEmpty();
  }

  @Test
  public void registeredTaskListenerKeepsCollectingMetrics() {
    sut.registerTaskListener();
    assertThat(sut.getTaskMetrics()).isEmpty();
    invokeJob();
    int size1 = sut.getTaskMetrics().size();
    assertThat(sut.getTaskMetrics()).isNotEmpty();
    invokeJob();
    int size2 = sut.getTaskMetrics().size();
    assertThat(size2).isGreaterThan(size1);
  }

  @Test
  public void unregisteredTaskListenerDoesNotCollect() {
    assertThat(sut.getTaskMetrics()).isEmpty();
    invokeJob();
    assertThat(sut.getTaskMetrics()).isEmpty();
  }

  @Test
  public void removedTaskListenerDoesNotCollect() {
    sut.registerTaskListener();
    sut.removeTaskListener();
    invokeJob();
    assertThat(sut.getTaskMetrics()).isEmpty();
  }

  @Test
  public void stageListenerReturnsList() {
    assertThat(sut.getStageInfo()).isEmpty();
  }

  @Test
  public void registeredStageListenerCollectsInfo() {
    sut.registerStageListener();
    assertThat(sut.getStageInfo()).isEmpty();
    invokeJob();
    assertThat(sut.getStageInfo()).isNotEmpty();
  }

  @Test
  public void unregisteredStageListenerDoesNotCollect() {
    assertThat(sut.getStageInfo()).isEmpty();
    invokeJob();
    assertThat(sut.getStageInfo()).isEmpty();
  }

  @Test
  public void removedStageListenerDoesNotCollect() {
    sut.registerStageListener();
    sut.removeStageListener();
    invokeJob();
    assertThat(sut.getStageInfo()).isEmpty();
  }
}
