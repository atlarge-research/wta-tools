package com.asml.apa.wta.spark;

import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerTaskStart;
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
   * @param args The first argument must be the path to the config file. The second argument must
   *             be the path to the resource file.
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("SystemTest");
    conf.set("spark.plugins", "com.asml.apa.wta.spark.WtaPlugin");
    System.setProperty("configFile", args[0]);
    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      testFile = sc.textFile(args[1]);
      sc.sc().addSparkListener(new SparkListener() {
        @Override
        public void onExecutorAdded(SparkListenerExecutorAdded event) {
          log.info("Executor added: {}", event.executorId());
        }

        @Override
        public void onTaskStart(SparkListenerTaskStart event) {
          log.info("Task started on executor: {}", event.taskInfo().executorId());
          log.info("Host: {}", event.taskInfo().host());
        }
      });
    } catch (Exception e) {
      log.error("Error occurred while creating spark context", e);
      log.info("Invoking Spark application without plugin");
    }
    for (int i = 0; i < 10; i++) {
      invokeJob();
    }
  }
}
