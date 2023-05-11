package com.asml.apa.wta.spark;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.spark.datasource.SparkDatasource;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.Arrays;

public class SparkDatasourceTest {

    private SparkSession spark;
    private SparkDatasource sut;

    private String resourcePath = "src/test/resources/wordcount.txt";
    private JavaRDD<String> testFile;

    @BeforeEach
    public void setup() {
        SparkConf conf = new SparkConf()
                .setAppName("SparkTestRunner")
                .setMaster("local[1]")
                // 1 executor per instance of each worker
                .set("spark.executor.instances", "1")
                // 2 cores on each executor
                .set("spark.executor.cores", "2");
        spark = SparkSession.builder().config(conf).getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        sut = new SparkDatasource(spark.sparkContext());
        testFile = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(resourcePath);
    }

    private void invokeJob() {
        testFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .collect();   // important to collect to store the metrics
    }

    @Test
    public void sparkDatasourceIsNotNull() {
        assertThat(sut).isNotNull();
    }

    @Test
    public void taskListenerReturnsList() {
        sut.getTaskMetrics();
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
    public void unregisteredTaskListenerDoesNotCollect() {
        assertThat(sut.getTaskMetrics()).isEmpty();
        invokeJob();
        assertThat(sut.getTaskMetrics()).isEmpty();
    }
}
