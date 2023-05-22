package com.asml.apa.wta.spark;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.datasource.IostatDataSource;
import com.asml.apa.wta.spark.datasource.SparkDataSource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

import com.asml.apa.wta.spark.executor.WtaExecutorPlugin;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import scala.Tuple2;
import static org.mockito.Mockito.*;
import java.io.IOException;
import java.util.List;


public class SparkDataSourceIntegrationTest {

  private SparkSession spark;
  private SparkDataSource sut;

  private JavaRDD<String> testFile;

  @BeforeEach
  public void setup() {
    SparkConf conf = new SparkConf()
        .setAppName("SparkTestRunner")
        .setMaster("local[1]")
        .set("spark.plugins", WtaPlugin.class.getName())
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
  public void taskListenerReturnsList() {
    assertThat(sut.getTaskMetrics()).isEmpty();
  }
  @Test
  public void testtest() throws IOException, InterruptedException {
    IostatDataSource a = new IostatDataSource();
    a.getAllMetrics();
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
