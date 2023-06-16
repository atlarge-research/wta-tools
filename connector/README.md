# Connectors

This folder contains submodules that are connectors that support the integration of different frameworks in
the adapters. Currently, we only support HDFS. A more detailed description of each connector is provided in the
source root of each adapter.

- [HDFS Connector](/connector/hdfs/README.md)

When using connectors together with the Apache Spark adapter through spark submit, the job should be submitted the
regular way, with one difference. This difference being that you also submit the connector jar, as the last argument.
The connector jar has to be the last one, order does matter.
