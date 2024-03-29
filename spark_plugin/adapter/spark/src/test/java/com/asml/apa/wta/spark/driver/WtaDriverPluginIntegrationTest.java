package com.asml.apa.wta.spark.driver;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
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

    conf.set("spark.plugins", pluginClass)
        .set("spark.driver.extraJavaOptions", "-DconfigFile=src/test/resources/config.json");
    SparkContext sc = SparkSession.builder().config(conf).getOrCreate().sparkContext();

    testFile = JavaSparkContext.fromSparkContext(sc).textFile(resourcePath);
    invokeJob();
    sc.stop();
    assertThat(Files.list(directoryPath).findAny()).isNotEmpty();

    AtomicInteger fileCount = new AtomicInteger();
    Files.walk(directoryPath).forEach(path -> {
      if (Files.isRegularFile(path)) {
        fileCount.getAndIncrement();
      }
    });

    assertThat(fileCount.get()).isEqualTo(5);
  }

  @Test
  void pluginNotInitialisedByInvalidConfigFile() throws IOException {
    assertThat(Files.isDirectory(directoryPath)).isTrue();
    assertThat(Files.list(directoryPath).findAny()).isEmpty();

    conf.set("spark.plugins", pluginClass).set("spark.driver.extraJavaOptions", "nonExistingFile.json");
    SparkContext sc = SparkSession.builder().config(conf).getOrCreate().sparkContext();

    testFile = JavaSparkContext.fromSparkContext(sc).textFile(resourcePath);
    invokeJob();
    sc.stop();
    assertThat(Files.list(directoryPath).findAny()).isEmpty();
  }

  @Test
  void pluginNotInitialisedByUnsetConfig() throws IOException {
    assertThat(Files.isDirectory(directoryPath)).isTrue();
    assertThat(Files.list(directoryPath).findAny()).isEmpty();

    conf.set("spark.plugins", pluginClass);
    SparkContext sc = SparkSession.builder().config(conf).getOrCreate().sparkContext();

    testFile = JavaSparkContext.fromSparkContext(sc).textFile(resourcePath);
    invokeJob();
    sc.stop();
    assertThat(Files.list(directoryPath).findAny()).isEmpty();
  }
}
