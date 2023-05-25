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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * Entry point for the Spark plugin. Current main method is used for full system
 * testing purposes.
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
public class Main {

  private static JavaRDD<String> testFile;

  private static final String resourceDir = "./adapter/spark/src/main/resources/";

  private static final String testTextPath = resourceDir + "wordcount.txt";

  /**
   * Private method to run a very basic spark job.
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
   * Full plugin system testing. Sets the filepath and reads the user config file along
   * with output dir for generated parquet files. Then runs a simple spark job to
   * colelct metrics and uses the parquet utils to write to parquet.
   * @param args        Command line args. Expecting two arguments
   * @throws Exception  Possible Exception thrown from Parquet utils
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public static void main(String[] args) throws Exception {
    // 1. Set custom paths
    String configPath;
    String outputPath;
    try {
      configPath = args[0];
      outputPath = args[1];
    } catch (ArrayIndexOutOfBoundsException e) {
      configPath = resourceDir + "config.json";
      outputPath = resourceDir + "WTA";
    }

    // 2. create spark session and load config object
    SparkConf conf = new SparkConf()
            .setAppName("SparkRunner")
            .setMaster("local[1]")
            .set("spark.executor.instances", "2")
            .set("spark.executor.cores", "2");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("OFF");
    testFile = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(testTextPath);

    // 3. Register listeners
    SparkDataSource sut = new SparkDataSource(spark.sparkContext(), WtaUtils.readConfig(configPath));
    sut.registerTaskListener();
    sut.registerJobListener();
    sut.registerApplicationListener();

    // 4. invoke Spark job multiple times to generate metrics
    for (int i = 0; i < 100; i++) {
      invokeJob();
    }
    spark.stop();

    // 5. Use parquet utils to write to parquet
    String schemaVersion = "schema-1.0";
    ParquetWriterUtils parquetUtil = new ParquetWriterUtils(new File(outputPath), schemaVersion);

    // no resource object yet
    List<Task> tasks = sut.getTaskLevelListener().getProcessedObjects();
    List<Workflow> workFlow = sut.getJobLevelListener().getProcessedObjects();
    Workload workLoad =
        sut.getApplicationLevelListener().getProcessedObjects().get(0);

    // delete any potential existing parquet files
    new File(resourceDir + "WTA/resources/schema-1.0/resource.parquet").delete();
    new File(resourceDir + "WTA/tasks/schema-1.0/task.parquet").delete();
    new File(resourceDir + "WTA/workflows/schema-1.0/workflow.parquet").delete();
    new File(resourceDir + "WTA/workload/schema-1.0/workload.json").delete();

    // generate files
    parquetUtil.getTasks().addAll(tasks);
    parquetUtil.getWorkflows().addAll(workFlow);
    parquetUtil.readWorkload(workLoad);
    parquetUtil.writeToFile("resource", "task", "workflow", "workload");
  }
}
