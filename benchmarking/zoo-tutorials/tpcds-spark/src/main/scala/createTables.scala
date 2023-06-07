import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, SparkSession}
import com.databricks.spark.sql.perf.tpcds.TPCDSTables

object createTables {

  def main(args: Array[String]) {

    val dataDir = args(0) 
    val dsdgenDir = args(1)
    val scaleFactor = args(2)

    val databaseName = "tpcds"
    val format = "parquet"

    val spark = SparkSession.builder.appName("TPC-DS TableGen").enableHiveSupport().getOrCreate()
    val sqlContext = spark.sqlContext
   
    val tables = new TPCDSTables(sqlContext, dsdgenDir = dsdgenDir, scaleFactor = scaleFactor, useDoubleForDecimal = false, useStringForDate = false)
    val dbExists = spark.sql(s"show databases like '$databaseName'").count > 0
    if (!dbExists) {
      spark.sql(s"create database $databaseName")
    }
    tables.createExternalTables(dataDir, format, databaseName, overwrite = true, discoverPartitions = true)

    spark.stop()
  }
}
