package com.asml.apa.wta.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
   * Private method to invoke the Spark application with complex jobs involving partitions shuffling. Results don't
   * matter as the purpose is to generate Spark tasks with multiple parent-child relations and jobs with diverse
   * number of tasks for generated traces.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private static void sparkOperation() {
    JavaRDD<String> words =
        testFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
    JavaRDD<String> upperCaseWords =
        words.filter(word -> word.contains("harry")).map(String::toUpperCase);

    JavaPairRDD<String, Tuple2<Integer, Integer>> wordCountPairs = upperCaseWords
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey(Integer::sum)
        .mapToPair(tuple ->
            new Tuple2<>(tuple._1(), new Tuple2<>(tuple._1().length(), tuple._2())));
    JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> joinedPairs = wordCountPairs
        .cartesian(wordCountPairs)
        .filter(tuple -> tuple._1()._1().compareTo(tuple._2()._1()) < 0)
        .mapToPair(tuple -> new Tuple2<>(
            new Tuple2<>(tuple._1()._1(), tuple._2()._1()),
            new Tuple2<>(
                tuple._1()._2()._1() + tuple._2()._2()._1(),
                tuple._1()._2()._2() + tuple._2()._2()._2())));
    JavaPairRDD<Double, Tuple2<String, String>> sortedPairs = joinedPairs
        .filter(tuple -> tuple._2()._2() > 3)
        .mapValues(tuple -> (double) tuple._1() / tuple._2())
        .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
        .repartition(10)
        .sortByKey(false);
    JavaRDD<Tuple2<Double, Tuple2<String, String>>> tupled = sortedPairs.mapPartitionsWithIndex(
        (index, iter) -> {
          List<Tuple2<Double, Tuple2<String, String>>> tuples = new ArrayList<>();
          while (iter.hasNext()) {
            tuples.add(new Tuple2<>(
                iter.next()._1(),
                new Tuple2<>(
                    index + "-" + iter.next()._2()._1(),
                    iter.next()._2()._2())));
          }
          return tuples.iterator();
        },
        true);
    JavaRDD<Tuple2<Double, String>> mappedPairs = tupled.mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2()))
        .groupByKey()
        .filter(pair -> {
          return pair._1() >= 10;
        })
        .map(pair -> {
          String concatenated = "";
          for (Tuple2<String, String> value : pair._2()) {
            concatenated += value._1() + ":" + value._2() + ",";
          }
          return new Tuple2<Double, String>(pair._1(), concatenated);
        })
        .repartition(20);
    JavaRDD<String> strings = mappedPairs
        .mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()))
        .reduceByKey(Double::sum)
        .filter(tuple -> tuple._2() > 0.5)
        .sortByKey()
        .map(t -> t._1);

    wordCountPairs = strings.mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((a, b) -> a + b)
        .mapToPair(tuple ->
            new Tuple2<>(tuple._1(), new Tuple2<>(tuple._1().length(), tuple._2())));
    joinedPairs = wordCountPairs
        .cartesian(wordCountPairs)
        .filter(tuple -> tuple._1()._1().compareTo(tuple._2()._1()) < 0)
        .mapToPair(tuple -> new Tuple2<>(
            new Tuple2<>(tuple._1()._1(), tuple._2()._1()),
            new Tuple2<>(
                tuple._1()._2()._1() + tuple._2()._2()._1(),
                tuple._1()._2()._2() + tuple._2()._2()._2())));
    sortedPairs = joinedPairs
        .filter(tuple -> tuple._2()._2() > 3)
        .mapValues(tuple -> (double) tuple._1() / tuple._2())
        .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
        .repartition(10)
        .sortByKey(false);
    tupled = sortedPairs.mapPartitionsWithIndex(
        (index, iter) -> {
          List<Tuple2<Double, Tuple2<String, String>>> tuples = new ArrayList<>();
          while (iter.hasNext()) {
            tuples.add(new Tuple2<>(
                iter.next()._1(),
                new Tuple2<>(
                    index + "-" + iter.next()._2()._1(),
                    iter.next()._2()._2())));
          }
          return tuples.iterator();
        },
        true);
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
    SparkConf conf = new SparkConf()
        .setAppName("SystemTest")
        .setMaster("local")
        .set("spark.sql.shuffle.partitions", "500")
        .set("spark.plugins", "com.asml.apa.wta.spark.WtaPlugin");
    System.setProperty("configFile", args[0]);
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    SparkContext sc = spark.sparkContext();
    testFile = JavaSparkContext.fromSparkContext(sc).textFile(args[1]);
    sparkOperation();
    sc.stop();
  }
}
