import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, SparkSession}
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.functions._

object TPCDSBenchmark {

  def main(args: Array[String]) {

    val resultLocation = args(0) 
    val queryNums: Array[String] = if (args.length > 1) {
      args.slice(1, args.length)
    } else {
      Array[String]("all")
    }

    val spark = SparkSession.builder.appName("TPC-DS Benchmark").enableHiveSupport().getOrCreate()
    val sqlContext = spark.sqlContext

    val tpcds = new TPCDS (sqlContext = sqlContext)
    val databaseName = "tpcds"
    spark.sql(s"use $databaseName")
    val iterations = 1
    val queries = new Tpcds_Queries(queryNums).tpcds_Queries
    val timeout = 24*60*60
    
    val experiment = tpcds.runExperiment(
      queries,
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)
    experiment.waitForFinish(timeout)
    
    val result = experiment.getCurrentResults.withColumn("Name", substring(col("name"), 2, 100)).withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0).selectExpr("Name", "Runtime")
    result.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(resultLocation + "/performance")

    spark.stop()
  }
}

