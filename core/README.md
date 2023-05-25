# Core Module

This module contains all classes and auxillary data structures that are common in between all data sources such as Spark and Flink.

This module consists of:
 - Configuration that the user must specify that is required for the WTA format (authors,descriptions,etc).
 - Model objects (e.g. Resource,Task,Workflow,Workload)
 - Stream Infrastructure for the smart serialization/deserialization of objects such that memory is not a bottleneck.
 - Functionality for serializing into parquet

### WTA Model
Model objects which are representative of the final Parquet output.

### Stream infrastructure
This sets up the stream infrastructure for storing (intermediate) metrics from the spark.
The Stream is a single linked list storing streams of metric data. It will serialize the data into the disk
(using ObjectOutputStream in native Java to .ser extension files labeled by the current time in the system)
automatically after receiving certain amount of metrics(by default 1800), and will automatically deserialize the data
once the data is required(e.g. poll() method for the stream).


### IO Utilities
To read the config JSON file, we utilize the ObjectMapper from the Jackson library to parse it and create RuntimeConfig objects within the application. The resulting data will be written into a Parquet file for the Resource, Task, and Workflow information, while the Workload information will be written into a JSON file. Our approach involves the use of Jackson, Apache Avro, and Apache Hadoop.

The Utility class performs a preliminary check on the list of WTA (Workload, Task, and Activity) objects. It verifies whether any member of the list contains uninitialized values, indicated by -1 or -1.0 for basic types and null for object types. If any object in the list has such uninitialized values, the entire column associated with that object will be excluded during the schema creation process and in the final output.

Consequently, the object will be transformed into a Record format specific to the schema that has been constructed. Finally, the AvroUtils writer will handle the task of feeding the data to be written using the schema and AvroUtils.

#### Hadoop Dependency in Windows
Since Avro uses Hadoop, if you want to run the plugin in Windows, you need
to manually configure the environmental variable for the hadoop file system.
If you want to use a different hadoop version, you need to download the corresponding Winutils.exe for the
corresponding version of hadoop, put it in directory

path_to_hadoop_binary/bin/Winutils.exe

and add the environmental variable

HADOOP_HOME=path_to_hadoop_bin/hadoop

If you want to use a different executable, replace the Winutils.exe at <project_directory>/core/resources/hadoop
