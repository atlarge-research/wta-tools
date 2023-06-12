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
   * Private method to invoke a Spark job with complex partitions shuffling.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  private static void sparkOperations() {
    JavaRDD<String> words =
        testFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
    JavaRDD<String> wordsWithSpark = words.filter(word -> word.contains("harry"));
    JavaRDD<String> upperCaseWords = wordsWithSpark.map(String::toUpperCase);


    JavaPairRDD<String, Tuple2<Integer, Integer>> wordCountPairs =
            upperCaseWords.mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((a, b) -> a + b)
                    .mapToPair(tuple -> new Tuple2<>(tuple._1(), new Tuple2<>(tuple._1().length(), tuple._2())));
    wordCountPairs.filter(pair -> pair._2()._2() > 1).keys();
    JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> joinedPairs1 =
            wordCountPairs.cartesian(wordCountPairs)
                    .filter(tuple -> tuple._1()._1().compareTo(tuple._2()._1()) < 0)
                    .mapToPair(tuple -> new Tuple2<>(new Tuple2<>(tuple._1()._1(), tuple._2()._1()),
                            new Tuple2<>(tuple._1()._2()._1() + tuple._2()._2()._1(),
                                    tuple._1()._2()._2() + tuple._2()._2()._2())));
    JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> frequentPairs =
            joinedPairs1.filter(tuple -> tuple._2()._2() > 3);
    JavaPairRDD<Tuple2<String, String>, Double> averageLengthPairs =
            frequentPairs.mapValues(tuple -> (double) tuple._1() / tuple._2());
    JavaPairRDD<Double, Tuple2<String, String>> swappedPairs =
            averageLengthPairs.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));
    JavaPairRDD<Double, Tuple2<String, String>> repartitionedPairs =
            swappedPairs.repartition(10);
    JavaPairRDD<Double, Tuple2<String, String>> sortedPairs =
            repartitionedPairs.sortByKey(false);
    JavaRDD <Tuple2<Double, Tuple2<String, String>>> tupled =
            sortedPairs.mapPartitionsWithIndex((index, iter) -> {
              List<Tuple2<Double, Tuple2<String, String>>> tuples = new ArrayList<>();
              while (iter.hasNext()) {
                tuples.add(new Tuple2<>(iter.next()._1(), new Tuple2<>(index + "-" + iter.next()._2()._1(), iter.next()._2()._2())));
              }
              return tuples.iterator();
            }, true);
    JavaPairRDD<Double, Tuple2<String, String>> indexedPairs = tupled.mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2()));
    JavaPairRDD<Double, Iterable<Tuple2<String, String>>> groupedPairs = indexedPairs.groupByKey();
    JavaPairRDD<Double, Iterable<Tuple2<String, String>>> filteredPairs = groupedPairs.filter(pair -> {
      return pair._1() >= 10;
    });
    JavaRDD <Tuple2<Double, String>> mappedPairs = filteredPairs.map(pair -> {
      String concatenated = "";
      for (Tuple2<String, String> value : pair._2()) {
        concatenated += value._1() + ":" + value._2() + ",";
      }
      return new Tuple2<Double, String>(pair._1(), concatenated);
    }).repartition(20);
    JavaPairRDD<String, Double> pairRDD = mappedPairs.mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()));
    JavaPairRDD<String, Double> countRDD = pairRDD.reduceByKey((a, b) -> a + b).filter(tuple -> tuple._2() > 0.5);
    JavaPairRDD<String, Double> sortedRDD = countRDD.sortByKey();
    JavaRDD<String> strings = sortedRDD.reduceByKey((a, b) -> a + b).map(t -> t._1);



    JavaPairRDD<String, Tuple2<Integer, Integer>> wordCountPairs2 =
            strings.mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((a, b) -> a + b)
                    .mapToPair(tuple -> new Tuple2<>(tuple._1(), new Tuple2<>(tuple._1().length(), tuple._2())));
    wordCountPairs2.filter(pair -> pair._2()._2() > 1).keys();
    JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> joinedPairs3 =
            wordCountPairs2.cartesian(wordCountPairs2)
                    .filter(tuple -> tuple._1()._1().compareTo(tuple._2()._1()) < 0)
                    .mapToPair(tuple -> new Tuple2<>(new Tuple2<>(tuple._1()._1(), tuple._2()._1()),
                            new Tuple2<>(tuple._1()._2()._1() + tuple._2()._2()._1(),
                                    tuple._1()._2()._2() + tuple._2()._2()._2())));
    JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> frequentPairs2 =
            joinedPairs3.filter(tuple -> tuple._2()._2() > 3);
    JavaPairRDD<Tuple2<String, String>, Double> averageLengthPairs2 =
            frequentPairs2.mapValues(tuple -> (double) tuple._1() / tuple._2());
    JavaPairRDD<Double, Tuple2<String, String>> swappedPairs2 =
            averageLengthPairs2.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));
    JavaPairRDD<Double, Tuple2<String, String>> repartitionedPairs2 =
            swappedPairs2.repartition(10);
    JavaPairRDD<Double, Tuple2<String, String>> sortedPairs2 =
            repartitionedPairs2.sortByKey(false);
    JavaRDD <Tuple2<Double, Tuple2<String, String>>> tupled2 =
            sortedPairs2.mapPartitionsWithIndex((index, iter) -> {
              List<Tuple2<Double, Tuple2<String, String>>> tuples = new ArrayList<>();
              while (iter.hasNext()) {
                tuples.add(new Tuple2<>(iter.next()._1(), new Tuple2<>(index + "-" + iter.next()._2()._1(), iter.next()._2()._2())));
              }
              return tuples.iterator();
            }, true);
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
                    .setMaster("local[*]")
                    .set("spark.sql.shuffle.partitions", "500") // increase number of shuffle partitions to distribute workload more evenly across the cluster.
                    .set("spark.executor.instances", "2")
                    .set("spark.executor.cores", "2")
                    .set("spark.executor.memory", "4g")
                    .set("spark.plugins", "com.asml.apa.wta.spark.WtaPlugin");
    System.setProperty("configFile","adapter/spark/src/test/resources/config.json");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    SparkContext sc = spark.sparkContext();
    testFile = JavaSparkContext.fromSparkContext(sc).textFile("adapter/spark/src/test/resources/e2e-input.txt");
    sparkOperations();
    sc.stop();
  }
}
