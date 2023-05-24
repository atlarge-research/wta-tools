# Benchmarking Module

This module provides all the tools we use for benchmarking the plugin, these tools are also potentially useful for
anyone who wants to utilize this plugin as a way to evaluate its performance. Currently, it consists of:

### DAS scripts
We uses Distributed ASCI Supercomputer (DAS) system for the benchmarking of our plugin
(https://asci.tudelft.nl/project-das/)

The DAS scripts are python scripts that request for DAS
usage, configure the spark environment on it remotely.

It is based on the work of Tim Hegeman, Chris Lemaire, and Laurens Versluis
The github repo for the original script: https://github.com/lfdversluis/das-bigdata-deployment

### sparkMeasure
This is the spark benchmarking class based on jmh and the industrial standard TPC-DS benchmarking
(https://www.tpc.org/tpcds/)
It will run several iterations of queries on spark jobs and return the average metric of the results.