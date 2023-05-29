# Adapters

This folder contains submodules that are adapters that support the collection of data from different
Big Data Applications. Currently, we only support Spark. A more detailed description of each adapter is provided in the
source root of each adapter.

- [Spark Adapter Documentation ](/adapter/spark/README.md)

## Running the plugin
This plugin is meant to run via **spark-submit** along side a Spark application in JAR file. It is important that the
main Spark application is invoked as such:

```
spark-submit --class org.example.Main --master local[1] 
--conf spark.plugins=com.asml.apa.wta.spark.WtaPlugin 
--conf "spark.driver.extraJavaOptions=-DconfigFile=/home/philly/config.json -DoutputPath=/home/philly/WTA" 
--jars /home/philly/SparkPlugin.jar /home/philly/SimpleSpark.jar 
/home/philly/wordcount.txt
```

The first conf registers the main plugin class within the Spark plugin. The second conf provides the environment
variables that the plugin needs to run. The first variable is the path to the configuration file. The second variable
is the path for the output files. Then the jars options are passed with the Spark plugin jar and the main Spark
application jar.