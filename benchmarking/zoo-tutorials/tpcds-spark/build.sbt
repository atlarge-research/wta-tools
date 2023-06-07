
name := "TPCDS-Benchmark"

version := "0.1"

scalaVersion := "2.12.1"

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

sparkVersion := "3.2.4"

sparkComponents ++= Seq("sql", "hive")

lazy val `spark-sql-perf` = project 
lazy val root = (project in  file(".")).aggregate(`spark-sql-perf`).dependsOn(`spark-sql-perf`)
