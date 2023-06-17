# Core Module

This module contains all classes and auxiliary data structures that are common between all adapters.

This module consists of:
 - Essential configuration details for the WTA format, such as authors, descriptions, and other relevant information.
 - Model objects (e.g., Resource, Task, Workflow, Workload)
 - Stream Infrastructure for the smart serialisation/deserialisation of objects to prevent the driver from running out of memory.
 - Functionality for serialising into Parquet
 - Logic to collect resource utilization metrics.

## Streaming infrastructure
The general streaming infrastructure is used to store intermediate metrics before aggregation.

The Stream is a singly linked list storing streams of metric data. It will serialise the data to disk (using the `.ser` format) automatically after receiving a certain number of metrics (by default this is set to 1800). It will automatically deserialise the data once the data is required (e.g., when `poll()` is called).

## I/O Utilities
To read the config JSON file, we use the ObjectMapper from the Jackson library to parse it and create RuntimeConfig objects within the application. The resulting data will be written into a Parquet file for the Resource, Task, and Workflow information, while the Workload information will be written into a JSON file. Our approach involves the use of Jackson, Apache Avro, and Apache Hadoop.

The Utility class performs a preliminary check on the list of WTA (Workload, Task, and Activity) objects. It verifies whether any member of the list contains uninitialised values, indicated by -1 or -1.0 for basic types and null for object types. If any object in the list has such uninitialised values, the entire column associated with that object will be excluded during the schema creation process and in the final output.

Consequently, the object will be transformed into a Record format specific to the schema that has been constructed. Finally, the `AvroUtils` writer will handle the task of feeding the data to be written using the schema and AvroUtils.

### Logging

It is important that when using the `core` module to build adapter layers, `Log4j2Configuration#setUpLoggingConfig`
is invoked early to set the user-defined part of the logging configuration.
As of now, the user can define one part of the logging configuration using `core` logging, the log level. This
is set to `ERROR` by default, but the user can exert full control over this using the `config.json` file.

### SupplierExtractionEngine - Resource Utilization Metrics
The SupplierExtractionEngine is a class that is used to extract data from some suppliers. A `supplier` is an object that collects resource-utilization information
from a given source, such as the JVM or the command line.

#### Resource Utilization Metrics
- Create a new supplier class that implements the `InformationSupplier` interface. You can add logic to collect information from a new source.
- Add the supplier as a new dependency in `SupplierExtractionEngine`, if it is application specific, you may have to add the dependency in the application specific extraction engine, such as `SparkSupplierExtractionEngine`.
- Modify the `ping()` and `transform()` method in `SupplierExtractionEngine` to include information from the new supplier.
- Modify any relevant DTOs as necessary.

### Hadoop Dependency on Windows
Avro uses Hadoop internally. If you want to run the plugin in Windows, you need
to manually configure the environment variable for HDFS.
If you want to use a different Hadoop version, you need to download the relevant `WinUtils.exe` for the
correct version of Hadoop, and follow the instructions below:

- Replace the `WinUtils.exe` in the following directory.

```
path_to_Hadoop_binary/hadoop/bin/Winutils.exe
```
- Set the corresponding environment variable.

```
HADOOP_HOME=path_to_Hadoop_bin/hadoop
```

If you want to use a different executable, replace the Winutils.exe at `<project_directory>/core/resources/hadoop`
