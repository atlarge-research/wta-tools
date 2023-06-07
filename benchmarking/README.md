# Benchmarking Module
This module provides all the tools we use for benchmarking the Spark plugin. 

This module consists of:
- **das-bigdata-deployment**
- **zoo-tutorials** 

All the repositories are forked from the originals and modified to suit our needs. All rights and credits go to the original authors.

## das-bigdata-deployment
Script to download, configure, and deploy Hadoop HDFS, YARN, and Spark on the Distributed ASCII Supercomputer (DAS-5)

## zoo-tutorials
This module contains the code for running TPC-DS benchmarks on Spark. To run TPC-DS benchmarks on Spark, we only need the **tpcds-spark** submodule within **zoo-tutorials**, but it is necessary to clone the entire repo in order to properly package it using sbt.

The current instructions specified in the **tpcds-spark/README.md** file is meant for running it locally. In order to run it on DAS-5, make use of the **das-bigdata-deployment** module and modify accordingly.
