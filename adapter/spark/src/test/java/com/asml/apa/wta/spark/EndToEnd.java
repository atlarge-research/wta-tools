package com.asml.apa.wta.spark;

import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.utils.ParquetWriterUtils;
import com.asml.apa.wta.core.utils.WtaUtils;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import java.io.File;
import java.util.Arrays;
import java.util.List;
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
        .reduceByKey((a, b) -> a + b)
        .collect();
  }

  /**
   * Sets the filepath and reads the user config file along
   * with output dir for generated parquet files. Then runs a simple spark job to
   * colelct metrics and uses the parquet utils to write to parquet. User must provide their own
   * config file and output directory. Otherwise, system will exit.
   * @param args        Command line arguments for the config, output and text path. Expecting three arguments
   * @throws Exception  Possible Exception thrown from Parquet utils
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public static void main(String[] args) throws Exception {
    // 1. get filepath and directory path arguments from command line
    String configPath = "";
    String outputPath = "";
    String testTextPath = "";

    try {
      configPath = args[0];
      outputPath = args[1];
      testTextPath = args[2];
    } catch (ArrayIndexOutOfBoundsException e) {
      System.exit(1);
    }

    // 2. delete any potentially pre-existing parquet files
    String schemaVersion = "schema-1.0";
    new File(outputPath + "/resources/" + schemaVersion + "/resource.parquet").delete();
    new File(outputPath + "/tasks/" + schemaVersion + "/task.parquet").delete();
    new File(outputPath + "/workflows/" + schemaVersion + "/workflow.parquet").delete();
    new File(outputPath + "/workload/" + schemaVersion + "/generic_information.json").delete();

    // 3. create spark session and load config object
    SparkConf conf = new SparkConf().setAppName("SystemTest").setMaster("local");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    SparkContext sc = spark.sparkContext();
    testFile = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(testTextPath);

    // 4. Register listeners
    SparkDataSource sut = new SparkDataSource(sc, WtaUtils.readConfig(configPath));
    sut.registerTaskListener();
    sut.registerJobListener();
    sut.registerApplicationListener();

    // 5. invoke a simple Spark job multiple times to generate metrics
    for (int i = 0; i < 100; i++) {
      invokeJob();
    }
    sc.stop();

    // 6. use parquet utils to get metric objects
    ParquetWriterUtils parquetUtil = new ParquetWriterUtils(new File(outputPath), schemaVersion);
    List<Task> tasks = sut.getTaskLevelListener().getProcessedObjects();
    List<Workflow> workFlow = sut.getJobLevelListener().getProcessedObjects();
    Workload workLoad =
        sut.getApplicationLevelListener().getProcessedObjects().get(0);

    // 7. generate output
    parquetUtil.getTasks().addAll(tasks);
    parquetUtil.getWorkflows().addAll(workFlow);
    parquetUtil.readWorkload(workLoad);
    parquetUtil.writeToFile("resource", "task", "workflow", "generic_information");
  }
}
