# Adapter Module

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
