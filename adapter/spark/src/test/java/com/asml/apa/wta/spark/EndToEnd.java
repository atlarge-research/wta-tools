package com.asml.apa.wta.spark;

import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
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
@Slf4j
public class EndToEnd {

  private static JavaRDD<String> testFile;

  /**
   * Private method to invoke a Spark job with complex partitions shuffling.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private static void invokeJob() {
    JavaRDD<String> words = testFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
    JavaRDD<String> wordsWithSpark = words.filter(word -> word.contains("Spark"));
    JavaRDD<String> upperCaseWords = wordsWithSpark.map(String::toUpperCase);
    upperCaseWords.count();
    upperCaseWords.reduce((word1, word2) -> word1.length() > word2.length() ? word1 : word2).length();
    JavaPairRDD<String, Integer> wordLengthPairs = upperCaseWords.mapToPair(word -> new Tuple2<>(word, word.length()));
    JavaPairRDD<String, Tuple2<Integer, Integer>> joinedPairs = wordLengthPairs.join(wordLengthPairs);
    JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groupedPairs = joinedPairs.groupByKey();
    groupedPairs.reduceByKey((a, b) -> a).collect();
  }

  /**
   * Entry point for the e2e test. This method will create a spark session along with the plugin.
   * The 'configFile' environment variable must be specified. Even if an error occurs on the plugin,
   * it will not shut down the entire Spark job.
   * @param args First argument must be filepath to config file. Second argument must be filepath to
   *             resources file.
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("SystemTest").setMaster("local[1]");
    conf.set("spark.plugins", "com.asml.apa.wta.spark.WtaPlugin");
    conf.set("spark.sql.shuffle.partitions", "500");  // increase number of shuffle partitions to distribute workload more evenly across the cluster.
    System.setProperty("configFile", args[0]);
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    SparkContext sc = spark.sparkContext();
    testFile = JavaSparkContext.fromSparkContext(sc).textFile(args[1]);
    invokeJob();
    sc.stop();
  }
}
