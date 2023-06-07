# Benchmarking
This directory provides all the tools we use for benchmarking the Spark plugin. These tools are independent of the Spark plugin itself, and can be used to benchmark any Spark application. Furthermore these will *not* be packaged with the Spark plugin.

## Directory structure
This directory contains the following tools/module:
- **das-bigdata-deployment**
- **zoo-tutorials** 

All the tools are forked from its original repositories and modified to suit our needs. All rights and credits go to the original authors.

### das-bigdata-deployment
Script to download, configure, and deploy Hadoop HDFS, YARN, and Spark on the Distributed ASCII Supercomputer (DAS-5)

### zoo-tutorials
This module contains the code for running TPC-DS benchmarks on Spark. To run TPC-DS benchmarks on Spark, we only need the **tpcds-spark** submodule within **zoo-tutorials**, but it is necessary to clone the entire repo in order to properly package it using sbt.

The current instructions specified in the **tpcds-spark/README.md** file is meant for running it locally. In order to run it on DAS-5, make use of the **das-bigdata-deployment** module and modify accordingly.

### Downloading the modules
As these modules are independent tools from the Spark plugin, they are not included in the plugin itself. To download each subdirectory separately and not the entire repository, follow the instructions below:

1. Create a new directory for the repository and navigate into it
```bash
mkdir my_project
cd my_project
```

2. Initialize an empty Git repository and add the URL of this remote repository.
```bash
git init
git remote add -f origin https://github.com/user/repo.git
```

3. Enable sparse checkout
```bash
git config core.sparseCheckout true
```

4. Specify the subdirectory you want to checkout. In this example, assume you want to download the `big_data_deployment`  and `zoo-tutorials` subdirectories
```bash
echo "big_data_deployment/*" > .git/info/sparse-checkout
echo "zoo-tutorials/*" > .git/info/sparse-checkout
```

5. Finally, pull content from remote repo:
```bash
git pull origin main
```