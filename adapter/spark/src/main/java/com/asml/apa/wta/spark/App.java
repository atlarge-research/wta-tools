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
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

/**
 * Entry point for the Spark plugin. Used as part of the live demo along with spark-submit
 */
public class App {

  private static JavaRDD<String> testFile;

  private static void invokeJob() throws StreamingQueryException {
    System.out.println("Invoking job");
    testFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((a, b) -> a + b)
            .collect();
  }

  public static void main(String[] args) throws Exception {
    // Part 1: Spark code
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

    // 2. create spark session and load config object
    SparkConf conf = new SparkConf()
            .setAppName("App")
            .setMaster("local");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    SparkContext sc = spark.sparkContext();
    testFile = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(testTextPath);

    // Part 2: our code
    // 3. Register listeners
    SparkDataSource sut = new SparkDataSource(sc, WtaUtils.readConfig(configPath));

    // TODO: solve in taskListener java.lang.NoSuchMethodError:
    //  'scala.collection.immutable.Seq org.apache.spark.scheduler.SparkListenerJobStart.stageInfos()'
    sut.registerTaskListener();
    sut.registerJobListener();
    sut.registerApplicationListener();

    // 4. invoke Spark job multiple times to generate metrics
    for (int i = 0; i < 100; i++) {
      invokeJob();
    }
    sc.stop();

    // 5. Use parquet utils to write to parquet
    String schemaVersion = "schema-1.0";
    ParquetWriterUtils parquetUtil = new ParquetWriterUtils(new File(outputPath), schemaVersion);

    // no resource object yet
    List<Task> tasks = sut.getTaskLevelListener().getProcessedObjects();
    List<Workflow> workFlow = sut.getJobLevelListener().getProcessedObjects();
    Workload workLoad =
            sut.getApplicationLevelListener().getProcessedObjects().get(0);

    // generate files
    parquetUtil.getTasks().addAll(tasks);
    parquetUtil.getWorkflows().addAll(workFlow);
    parquetUtil.readWorkload(workLoad);
    parquetUtil.writeToFile("resource", "task", "workflow", "workload");
  }
}
