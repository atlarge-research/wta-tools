# Core Module

This module contains part of the plugin that we consider indispensable for the plugin. 
That is to say, it is the part of plugin that will not change even someone extends the plugin.

This module consists of:
 - configuration on the Spark-unrelated infos (authors,descriptions,etc)
 - definition of the internal WTA objects (e.g. Resource,Task,Workflow,Workload)
 - stream infrastructure for the serialization/deserialization of metrics
 - utility class providing read for configs and output to parquet files

### RuntimeConfiguration 
The runtime config stores information that is not included in the Spark job.
These information needs to be input by the user inside the config file

This includes the authors, domain(scientific/engineering/company purpose), description, events and the log level
(How detailed does the user want the logging to be).

### Internal WTA objects
These are the data objects of the corresponding WTA objects. 
These objects are the aggregation results from the spark metrics.
These objects implement BaseTraceObject interface for the benefit of future extendability.

### Stream infrastructure
This sets up the stream infrastructure for storing (intermediate) metrics from the spark.
The Stream is a single linked list storing streams of metric data. It will serialize the data into the disk
(using ObjectOutputStream in native Java to .ser extension files labeled by the current time in the system)
automatically after receiving certain amount of metrics(by default 1800), and will automatically deserialize the data
once the data is required(e.g. poll() method for the stream).

the (de)serialization is synchronized 
In addition, there is KeyedStream which allows each node content in the stream to carry a key for different aggregation
with respect to the key.

### IO Utilities
For reading the config json file, we use ObjectMapper in the Jackson library to read it into RuntimeConfig 
objects inside the application. For the final output, it will write Resource, Task and Workflow into parquet file and 
Workload into json file. We use Jackson as well as Apache Avro and Apache Hadoop.
The Utility class will go through the list of WTA objects first, checking whether there exists list member with fields 
has uninitialized values (set to -1/-1.0 for basic types and null for object types). As long as one object in the list 
has these uninitialized values, the whole column will be dropped in the following schema building and the final output.
The object will thus be converted into Record for the writer with respect to the schema built. and finally fed to the
AvroUtils writer.

#### Important regarding Hadoop dependency
Since Avro essentially uses Hadoop, if you want to run the plugin(thus running the parquet writer) in Windows, you need
to manually configure the environmental variable for the hadoop file system. 
If you want to use a different hadoop version, you need to download the corresponding Winutils.exe for the 
corresponding version of hadoop, put it in directory 

<Something>/hadoop/bin/Winutils.exe 

and add the environmental variable 

HADOOP_HOME=<Something>/hadoop

In the hadoop we offered the Winutils.exe in path: <project_directory>/core/resources/hadoop

