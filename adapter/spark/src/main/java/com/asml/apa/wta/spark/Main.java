package com.asml.apa.wta.spark;

import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.utils.ParquetWriterUtils;
import com.asml.apa.wta.core.utils.WtaUtils;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Entry point for the Spark plugin.
 */
public class Main {

  private static SparkSession spark;

  private static SparkDataSource sut;

  private static JavaRDD<String> testFile;

  private static ParquetWriterUtils parquetUtil;

  private static String resourceDir = "adapter/spark/src/main/resources/";

  private static String testTextPath = resourceDir + "wordcount.txt";

  private static String configPath = resourceDir + "config.json";

  private static String outputPath = resourceDir + "WTA";

  private static String schemaVersion = "schema-1.0";

  private static void invokeJob() {
    testFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((a, b) -> a + b)
            .collect(); // important to collect to store the metrics
  }

  public static void main(String[] args) throws Exception {
    // 1. Set paths
    try {
      configPath = args[0];
      outputPath = args[1];
    } catch (ArrayIndexOutOfBoundsException e) {
      System.out.println("No config path or output path specified, using default values");
    }

    // 2. create spark session and load config object
    SparkConf conf = new SparkConf()
            .setAppName("SparkRunner")
            .setMaster("local[1]")
            .set("spark.executor.instances", "2") // 1 executor per instance of each worker
            .set("spark.executor.cores", "2"); // 2 cores on each executor
    spark = SparkSession.builder().config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("OFF");
    sut = new SparkDataSource(spark.sparkContext(), WtaUtils.readConfig(configPath));
    testFile = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(testTextPath);

    // 3. Register listeners
    sut.registerTaskListener();
    sut.registerJobListener();
    sut.registerApplicationListener();

    // 4. Invoke a job multiple times and stop spark session when finished
    for (int i = 0; i < 1000; i++) {
      invokeJob();
    }
    spark.stop();

    // 5. Use parquet utils to write to parquet
    parquetUtil = new ParquetWriterUtils(new File(outputPath), schemaVersion);

    // no resource object yet
    List<Task> tasks = sut.getTaskLevelListener().getProcessedObjects();
    List<Workflow> workFlow = sut.getJobLevelListener().getProcessedObjects();
    Workload workLoad = sut.getApplicationLevelListener().getProcessedObjects().get(0);

    // first delete any existing parquet files
    new File(resourceDir + "WTA/resources/schema-1.0/resource.parquet").delete();
    new File(resourceDir + "WTA/tasks/schema-1.0/task.parquet").delete();
    new File(resourceDir + "WTA/workflows/schema-1.0/workflow.parquet").delete();
    new File(resourceDir + "WTA/workload/schema-1.0/workload.json").delete();

    // write
    parquetUtil.getTasks().addAll(tasks);
    parquetUtil.getWorkflows().addAll(workFlow);
    parquetUtil.readWorkload(workLoad);
    parquetUtil.writeToFile("resource", "task", "workflow", "workload");
  }
}
