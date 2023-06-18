package com.asml.apa.wta.spark.driver;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.io.DiskParquetInputFile;
import com.asml.apa.wta.core.io.ParquetReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

class WtaDriverPluginIntegrationTest {

  private SparkConf conf;

  private JavaRDD<String> testFile;

  private static final String TOOL_VERSION = "spark-wta-generator-1_0";

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

    Path taskPath = Path.of("wta-output", TOOL_VERSION, "tasks", "schema-1.0", "tasks.parquet");
    Path resourcesPath = Path.of("wta-output", TOOL_VERSION, "resources", "schema-1.0", "resources.parquet");
    Path resourceStatesPath =
        Path.of("wta-output", TOOL_VERSION, "resource_states", "schema-1.0", "resource_states.parquet");
    Path workflowPath = Path.of("wta-output", TOOL_VERSION, "workflows", "schema-1.0", "workflows.parquet");

    try (ParquetReader reader = new ParquetReader(new DiskParquetInputFile(taskPath))) {
      GenericRecord result = reader.read();
      AssertionsForClassTypes.assertThat(result.get("id")).isEqualTo(1L);
      result = reader.read();
      AssertionsForClassTypes.assertThat(result.get("id")).isEqualTo(2L);
    }

    try (ParquetReader reader = new ParquetReader(new DiskParquetInputFile(resourcesPath))) {
      GenericRecord result = reader.read();
      AssertionsForClassTypes.assertThat(result.get("id")).isEqualTo(1323526104L);
    }

    try (ParquetReader reader = new ParquetReader(new DiskParquetInputFile(resourceStatesPath))) {
      GenericRecord result = reader.read();
      AssertionsForClassTypes.assertThat(result.get("resource_id")).isEqualTo(1323526104L);
    }

    try (ParquetReader reader = new ParquetReader(new DiskParquetInputFile(workflowPath))) {
      GenericRecord result = reader.read();
      AssertionsForClassTypes.assertThat(result.get("id")).isEqualTo(1L);
    }
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
