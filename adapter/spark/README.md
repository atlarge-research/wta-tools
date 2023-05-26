# Spark Adapter Layer

## Installation and Usage
- Clone the repository
- Run `mvn -pl core clean install && mvn -pl adapter/spark clean package` in the source root.
- Copy the resulting jar file from `adapter/spark/target`.
- Execute the following command in the directory where the jar file is located:

```bash
spark-submit --class com.asml.apa.wta.spark.App
--master local[1] <plugin_jar_location> <config.json_location> <directory_for_outputted_parquet> <file_to_be_processed>
```
- The parquet files should now be located in the <directory_for_outputted_parquet>.

## Description

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
utilization metrics on the executor side. These metrics are then passed to the driver using the plugin API. Namely, we use `ask()`,`send(Object message)` and `receive(Object message)`.

Aggregation of all the resource utilization metrics are done at the driver's end.

## Guidelines for Developers
- If a new data source is added in the future, be sure to use the existing streaming infrastructure that handles `MetricsRecord`. This helps the driver in terms of memory usage.
- When a resource is not needed anymore, release it in `shutdown()`, within the respective `PluginContext`.
