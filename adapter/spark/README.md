# Spark Adapter Layer

## Overview

![img.png](architecture.png)

The Spark Adapter is responsible for parsing Spark execution information into WTA objects.
The diagram above illustrates the workflow of the adapter.

- **Label 1:** Heartbeat sent by the executor to the driver every 10 seconds to send metrics.
  These are intercepted by the `SparkListenerAPI`.
- **Label 2:** RPC messages sent using the `SparkPluginAPI` for executors to communicate
  any additional information to the driver-side of the plugin.
- **Label 3:** At each stage, the task scheduler gets sets of tasks from the DAG and the task scheduler
  sends the tasks to each executor.
- **Label 4:** Once the job has ended, all objects will be serialised into parquet format.

## Installation and Usage
1.  Clone the repository
2.  Optional (if more I/O metrics are needed):
   - Install the sysstat package by running the following command in the terminal:
     ```bash
     sudo apt install sysstat
     ```

   - Install the dstat package by running the following command in the terminal:
    ```bash
    sudo apt install dstat
    ```

3.  To allow advanced performance metrics to be gathered, you can opt to make the `perf` utility available.
    To do this, you need to do the following:

    On Ubuntu:

    ```bash
    apt-get install linux-tools-common
    apt-get install linux-tools-generic
    apt-get install linux-tools-`uname -r`
    ```

    On Debian:

    ```bash
    apt-get install linux-perf
    ```

    On CentOS / RHEL:

    ```bash
    yum install perf
    ```

    Followed by setting `perf_event_paranoid` to 0:

    ```bash
    sysctl -w kernel.perf_event_paranoid=0
    ```

    It is important to note that the installed version of `perf` must be compatible with the kernel. Especially for containerised environments, this could be an issue.

There are two ways to make use of the plugin
1. Integrate the plugin into the Spark application source code
2. Create the plugin as a JAR and run alongside the main Spark application via **spark-submit**

### Plugin Integration
For the first approach, create a `SparkConf` object and set the following config:

```java
conf.set("spark.plugins", "com.asml.apa.wta.spark.WtaPlugin");
System.setProperty("configFile", "adapter/spark/src/test/resources/config.json");
```
The first line registers the main plugin class within the Spark session. The second line creates an environment variable
for the plugin class to use.

### CLI Execution
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
- The Parquet files should now be located in the `outputPath` as specified in the config file.

Note: this way, the plugin will be compiled for Scala 2.12. If you want to compile for a Scala 2.13 version of Spark,
you will need to set the `spark.scala.version` flag to 2.13, such as in
`mvn -pl adapter/spark -Dspark.scala.version=2.13 clean package`.

## Configuration
General configuration instructions are located [here](/../../README.md#configuration). See above for [instructions](#installation-and-usage) on how to provide the configuration to the plugin.


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

## Developer Guidelines

### General Remarks
- When a resource is not needed anymore, release it in `shutdown()`, within the respective `PluginContext`.
- The WTA format uses ids in `INT64` format. The Spark API provides some ids (such as `executorID`) in string format. To convert this, we use `Math.abs(id.hashCode())`.
- All timestamps are in Unix epoch millis.

### Known Limitations

#### Utilisation of the plugin on Windows
- We don't recommend using the plugin on Windows, although it is possible.
- The plugin is compatible with Windows; however, the collection of resource utilization metrics is limited.
    - This limitation arises because the plugin relies on dependencies that are only available on UNIX-based systems.
- Logs will be generated indicating that certain resource utilization metrics cannot be collected.
  - This can potentially cause performance issues if the resource ping interval is small since the plugin will write to the log each time a non-available resource is pinged. I/O operations can be expensive.

#### Stage Level Metrics
- Stage level metrics are only outputted in the trace if they are submitted and successfully completed. The scheduler sometimes creates stages which are later skipped or removed.
- Stage level metrics are very sparse, this is a limitation with the Spark API itself.

#### Lombok Usage
- Due to the way Lombok works. It is not possible to call a superclass constructor from a subclass constructor. This is why things like this exist:
```java
@Override
public SparkBaseSupplierWrapperDto transform(BaseSupplierDto record) {
    return SparkBaseSupplierWrapperDto.builder()
        .executorId(pluginContext.executorID())
        .timestamp(record.getTimestamp())
        .osInfoDto(record.getOsInfoDto())
        .iostatDto(record.getIostatDto())
        .dstatDto(record.getDstatDto())
        .perfDto(record.getPerfDto())
        .jvmFileDto(record.getJvmFileDto())
        .procDto(record.getProcDto())
        .build();
}
```

#### Task-Level Resource metrics
- This is a big area of improvement for the plugin. Due to the limitations of the Spark API, we are not able to easily isolate resource level metrics (such as disk IO time), to specific tasks. We have had to make several compromises
  - Fields such as `energyConsumption`, relate to the energy consumption of the entire executor (during the lifespan of the task) and not the task itself.
  - TaskState information is not accurate. Data analysis on this object can produce inaccurate results.


## Benchmarking
[The benchmarking module](../../submodules/benchmarking/README.md) is used to benchmark the performance of the plugin. Any changes to the plugin should be benchmarked to ensure no significant performance degradation.

It is important to note that the benchmarking module is not part of the plugin itself but a separate tool.
