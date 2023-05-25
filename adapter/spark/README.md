# Spark Adapter Layer

The Spark Adapter consists of two main parts that allows the application to collect metrics.
- SparkListenerInterface
- SparkPlugin API

## SparkListenerInterface

The SparkListenerInterface is responsible for listening to the spark events and collect the metrics. As part of the
standard instrumentation of Spark, metrics are sent from the executor to the driver as part of a heartbeat. The listener interface
intercepts these events. Examples of how we use it, and what metrics we collect for the different WTA objects can be seen [here](/src/main/java/com/asml/apa/wta/spark/listener).

It also allows us to define custom behaviour when certain events are intercepted, such as tracking the various stage ids for a spark job.

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

Our main use case for the plugin API is to pass messages between the executor and the driver. We use different libraries such as `Iostat` to collect resource
utilization metrics on the executor side. These metrics are then passed to the driver using the plugin API. Namely, we use `ask()`,`send(Object message)` and `receive(Object message)`.

This module listens to the spark job that is carrying out, retrieves metric from it and
aggregate to the WTA objects. This module consists of:

- Spark plugin connector
- Datasources which collect metrics from different sources(e.g. spark native, PAPI, Promethus, NVIDIA API)
- Listeners with each belonging to a certain datasource
- Streams as a facade that utilize the stream infrastructure

### Plugin connector
We use the plugin api provided in native Spark to connect our plugin to the Spark job.
The plugin consists of two components: The driver plugin and the executor plugin.
Note that we only utilize driver plugin.

### Datasource
The datasource combines multiple listener that comes from a single source. For example, the SparkDataSource contains
listeners that uses native Spark apis on different levels. It also allows adding/removing listeners on runtime.

### Listeners
Listeners directly listen the metric from external apis. For example, spark listeners will return the list of metrics
upon the callbacks are made. In particular, on the application end callback, it will also send all info on
different listeners to the writer for the output of the trace format.

### Streams
For listeners with real-time metrics(This means the metric is not retrieved from the api once something is done but
instead constantly listens the metric in a certain time frequency), we need to aggregate the large amount of data into
a single metric. The streams provide a facade for such need. We will have a MetricStreamingEngine to manage all the
streams and create a keyed stream for each metric.