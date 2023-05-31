package com.asml.apa.wta.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

class WtaDriverPluginIntegrationTest {

  private SparkConf conf;

  private JavaRDD<String> testFile;

  private final String resourcePath = "src/test/resources/wordcount.txt";

  private final String pluginClass = "com.asml.apa.wta.spark.WtaPlugin";

  private final String validConfigFile = "src/test/resources/config.json";

  private final Path directoryPath = Path.of("wta-output");

  @BeforeEach
  void setup() throws IOException {
    Files.createDirectories(directoryPath);
    conf = new SparkConf().setAppName("DriverTest").setMaster("local");
  }

  @AfterEach
  void teardown() throws IOException {
    Files.walk(directoryPath)
        .sorted(java.util.Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(java.io.File::delete);
  }

  private void invokeJob() {
    testFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((a, b) -> a + b)
        .collect();
  }

  @Test
  void pluginInitialisedGeneratesOutput() throws IOException {
    assertThat(Files.isDirectory(directoryPath)).isTrue();
    assertThat(Files.list(directoryPath).findAny()).isEmpty();

    conf.set("spark.plugins", pluginClass);
    System.setProperty("configFile", validConfigFile);
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    SparkContext sc = spark.sparkContext();
    assertThat(sc.getConf().get("spark.plugins")).isEqualTo(pluginClass);

    testFile = JavaSparkContext.fromSparkContext(sc).textFile(resourcePath);
    invokeJob();
    sc.stop();
    assertThat(Files.list(directoryPath).findAny()).isNotEmpty();
  }

  @Test
  void pluginNotInitialisedByInvalidConfigFile() throws IOException {
    assertThat(Files.isDirectory(directoryPath)).isTrue();
    assertThat(Files.list(directoryPath).findAny()).isEmpty();
    conf.set("spark.plugins", pluginClass);

    System.setProperty("configFile", "nonExistingFile.json");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    SparkContext sc = spark.sparkContext();
    assertThat(spark.sparkContext().getConf().get("spark.plugins")).isEqualTo(pluginClass);

    testFile = JavaSparkContext.fromSparkContext(sc).textFile(resourcePath);
    invokeJob();
    sc.stop();
    assertThat(Files.list(directoryPath).findAny()).isEmpty();
  }
}
