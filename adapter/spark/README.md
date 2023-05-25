# Spark Adapter Layer

The Spark Adapter consists of two main parts that allows the application to collect metrics.
- SparkListenerInterface
- SparkPlugin API

## SparkListenerInterface

The SparkListenerInterface is responsible for listening to the Spark events and collect the metrics. As part of the
standard instrumentation of Spark, metrics are sent from the executor to the driver as part of a heartbeat. The listener interface
intercepts these events. Examples of how we use it, and what metrics we collect for the different WTA objects can be seen [here](/src/main/java/com/asml/apa/wta/spark/listener).

It also allows us to define custom behaviour when certain events are intercepted, such as tracking the various stage ids for a Spark job.

```java
@Override
public void onJobStart(SparkListenerJobStart jobStart) {
    jobStart.stageInfos().foreach(stageInfo -> stageIdsToJobs.put(stageInfo.stageId(), jobStart.jobId()));
}
```

## Spark Plugin API
We also use the Spark Plugin API to connect our plugin to the Spark job. The plugin consists of two components: The driver plugin and the executor plugin.
An instance of the `DriverPlugin` is created for the driver, it's lifecycle is equivalent to that of the Spark application. An instance of `ExecutorPlugin` gets instantiated
for each executor, and it's lifecycle is equivalent to that of an executor. This can possibly span multiple tasks.

Our main use case for the plugin API is to pass messages between the executor and the driver. We use different libraries such as `iostat` to collect resource
utilization metrics on the executor side. These metrics are then passed to the driver using the plugin API. Namely, we use `ask()`,`send(Object message)` and `receive(Object message)`.

Aggregation of all the resource utilization metrics are done on the driver side.

This module listens to the Spark job that is carrying out, retrieves metric from it and
aggregate to the WTA objects. This module consists of:


## Notes for Developers
- If a new data source has to be added be sure to use the existing streaming infrastructure that handles MetricRecords in a smart way. This helps the driver in terms of memory use out of memory.
- When a resource is not needed anymore, release it in `shutdown()`, within the respective `PluginContext`.
