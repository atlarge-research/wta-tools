package com.asml.apa.wta.spark;

import com.asml.apa.wta.spark.datasource.SparkDataSource;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class Main {
  public static void main(String[] args) {
    // System.out.println("Hello World!");
    SparkConf conf = new SparkConf()
            .setAppName("SparkTestRunner")
            .setMaster("local[1]")
            .set("spark.plugins", WtaPlugin.class.getName())
            .set("spark.executor.instances", "1") // 1 executor per instance of each worker
            .set("spark.executor.cores", "2"); // 2 cores on each executor

    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    SparkDataSource sut = new SparkDataSource(spark.sparkContext());
    String resourcePath = "/home/lyadalachanchu/asml-dev/adapter/spark/src/test/resources/wordcount.txt";
    JavaRDD<String> testFile = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(resourcePath);

    testFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((a, b) -> a + b)
            .collect(); // important to collect to store the metrics

    sut.registerTaskListener();
  }
}
