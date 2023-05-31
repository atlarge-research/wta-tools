package com.asml.apa.wta.spark;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * Used for full system testing the plugin. This class will be the entry point for the
 * JAR file when running with spark-submit.
 *
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class EndToEnd {

  private static JavaRDD<String> testFile;

  /**
   * Private method to invoke a simple Spark job.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private static void invokeJob() {
    testFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey(Integer::sum)
        .collect();
  }

  /**
   * Entry point for the e2e test. This method will create a spark session along with the plugin.
   * The 'configFile' environment variable must be specified. Even if an error occurs on the plugin,
   * it will not shut down the entire Spark job.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("SystemTest").setMaster("local");
    conf.set("spark.plugins", "com.asml.apa.wta.spark.WtaPlugin");
    System.setProperty("configFile", "adapter/spark/src/test/resources/config.json");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    SparkContext sc = spark.sparkContext();
    testFile = JavaSparkContext.fromSparkContext(sc).textFile("adapter/spark/src/test/resources/wordcount.txt");
    for (int i = 0; i < 100; i++) {
      invokeJob();
    }
    sc.stop();
  }
}
