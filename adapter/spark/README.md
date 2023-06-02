# Spark Adapter Layer

## Overview

![img.png](./src/main/resources/architecture.png)

The Spark Adapter is responsible for parsing Spark execution information into WTA objects.
The diagram above illustrates the workflow of the adapter. 

- **Label 1:** Heartbeat sent by the executor to the driver every 10 seconds to send metrics.
  These are intercepted by the `SparkListenerAPI`.
- **Label 2:** RPC messages sent using the `SparkPluginAPI` for executors to communicate
  any additional information to the driver-side of the plugin.
- **Label 3:** The task scheduler is responsible for sending tasks to each executor, at each stage,
  the task scheduler gets sets of tasks from the DAG to run. 
- **Label 4:** Once the job has ended, all objects will be serialised into parquet format.

## Installation and Usage
- Clone the repository
- Optional (if more I/O metrics are needed): Install sysstat by running the following bash command:

```bash
sudo apt install sysstat
```

There are two ways to make use of the plugin
1. Integrate the plugin into the Spark application source code
2. Create the plugin as a JAR and run alongside the main Spark application via **spark-submit**

### First approach
For the first approach, create a `SparkConf` object and set the following config:

```java
conf.set("spark.plugins", "com.asml.apa.wta.spark.WtaPlugin");
System.setProperty("configFile", "adapter/spark/src/test/resources/config.json");
```
The first line registers the main plugin class within the Spark session. The second line creates an environment variable
for the plugin class to use.

### Second approach
For the second approach, create a JAR file of the plugin and run it alongside the main Spark application using
**spark-submit**. Here is an example of how to run the plugin alongside the main Spark application:

- Run `mvn -pl core clean install && mvn -pl adapter/spark clean package` in the source root.
- Copy the resulting jar file from `adapter/spark/target`.
- Execute the following command in the directory where the jar file is located:

```bash
spark-submit --class <main class path to spark application> --master local[1]
--conf spark.plugins=com.asml.apa.wta.spark.WtaPlugin
--conf "spark.driver.extraJavaOptions=-DconfigFile=<config.json_location>"
--jars <plugin_jar_location> <Spark_jar_location>
<optional arguments for spark application>
```
- The parquet files should now be located in the `outputPath` as specified in the config file.

## Description
This plugin will **not** block the main Spark application. Even if the plugin fails to initialise, the main Spark
application will still run.

The Spark Adapter consists of two main parts that allows the application to collect metrics.
- SparkListenerInterface
- SparkPlugin API

This module listens to events from the Spark job that is being carrying out. It retrieves metrics and subsequently aggregates it to different WTA objects. The metrics are then stored in a parquet file.

### SparkListenerInterface

The SparkListenerInterface listens to the Spark events and collects the metrics. As part of the
standard instrumentation of Spark, metrics are transmitted from the executor to the driver as part of a heartbeat. The listener interface
intercepts these events. Examples of how we use it, and what metrics we collect for the different WTA objects can be seen [here](/src/main/java/com/asml/apa/wta/spark/listener).

It also allows us to define custom behaviour when certain events are intercepted, such as tracking the various stage ids for a Spark job.

```java
@Override
public void onJobStart(SparkListenerJobStart jobStart) {
    jobStart.stageInfos().foreach(stageInfo -> stageIdsToJobs.put(stageInfo.stageId(), jobStart.jobId()));
}
```

### Spark Plugin API
We also use the Spark Plugin API to connect our plugin to the Spark job. The plugin consists of two components: The driver plugin and the executor plugin.
An instance of the `DriverPlugin` is created for the driver, it's lifecycle is equivalent to that of the Spark application. An instance of `ExecutorPlugin` gets instantiated
for each executor, and it's lifecycle is equivalent to that of an executor. This can possibly span multiple tasks.

Our main use case for the plugin API is to pass messages between the executor and the driver. We use different libraries such as `iostat` to collect resource
utilisation metrics on the executor side. These metrics are then passed to the driver using the plugin API. Namely, we use `ask()`,`send(Object message)` and `receive(Object message)`.

Aggregation of all the resource utilisation metrics are done at the driver's end.

## Guidelines for Developers
- If a new data source is added in the future, be sure to use the existing streaming infrastructure that handles `MetricsRecord`. This helps the driver in terms of memory usage.
- When a resource is not needed anymore, release it in `shutdown()`, within the respective `PluginContext`.
