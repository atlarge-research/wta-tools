## This script (The DAS configuration script) is based on the work of Tim Hegeman, Chris Lemaire, and Laurens Versluis 
## The github repo for the original script: https://github.com/lfdversluis/das-bigdata-deployment

# Deployment scripts for big data frameworks on DAS-5
Scripts to download configure, and deploy several big data frameworks (YARN/MapReduce, Spark) and related systems (HDFS, ZooKeeper).

Warning: The scripts assume ownership of the `/local/$USER/` directory on every node used in a deployment. In particular, the scripts will wipe the `/local/$USER/{hadoop,spark,zookeeper,influxdb}` directory before the respective application is deployed.

## Installation
1. Git clone this repository to your home directory on DAS-5. The resulting directory will be referred to as `$DEPLOYER_HOME` throughout this manual.
2. Set the environment path variable to the `$DEPLOYER_HOME` and other required tools:

```shell
export PATH=$PATH:`<path_to_$DEPLOYER_HOME>`
export JAVA_HOME=`<path_to_JAVA>`
export PATH=$JAVA_HOME/bin:$PATH
```

Optional if space in your home directory is limited (deployments are likely to generate gigabytes of logs over time):

3. Create a directory in your scratch folder for the big data frameworks and configuration files, e.g., `/var/scratch/$USER/big-data-frameworks`.
4. Create a symlink in `$DEPLOYER_HOME` called `frameworks` pointing at the directory you created at point 2.

## Reserving nodes on DAS-5
The deployer can create a new reservation via `preserve` or you may use existing reservations. To create a reservation run:

```shell
module load prun
deployer preserve create-reservation -q -t "$TIMEOUT" $MACHINES
```

where `$TIMEOUT` should be the duration of the reservation in `hh:mm:ss` format and `$MACHINES` should be the number of nodes to reserve. A minimum of 2 nodes is required. The output includes the ID of your reservation.

Use the following (substituting your reservation ID) to check the status of your reservation:

```shell
preserve -list 
```

or

```shell
deployer preserve fetch-reservation $RESERVATION_ID
```

The first compute node specified is the master node.

To kill a specific reservation:

```shell
preserve -c $RESERVATION_ID
```

## Deploying frameworks
To get a list of supported frameworks and versions, run:

```shell
deployer list-frameworks --versions
```

Before a framework can be deployed, it must be "installed". This only needs to be done once. After installing, the framework can be repeatedly deployed. In the following command, substitute a framework name and version as output by the `deployer list-frameworks` command.

```shell
deployer install $FRAMEWORK $VERSION
```

To deploy a framework, use the `deployer deploy -h` command for help, or use one of the following standard deployments.

### Deploying Hadoop
Before deploying hadoop, add the following environment path variables:

```shell
export HADOOP_HOME=`<path_to_installed_hadoop>`
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
```

To deploy Hadoop (HDFS and YARN) with sensible defaults, run the following command (substituting your reservation ID):

```shell
cd das-bigdata-deployment
deployer deploy --preserve-id $RESERVATION_ID -s env/das5-hadoop.settings hadoop 3.2.4
```

If you do not need HDFS or YARN append the `hdfs_enable=false` or `yarn_enable=false` options, respectively, to the above comand.

### Deploying Spark
To deploy Spark with sensible defaults, run the following command (substituting your reservation ID):

```shell
cd das-bigdata-deployment
deployer deploy --preserve-id $RESERVATION_ID -s env/das5-spark.settings spark 3.2.4
```

### HDFS
It is recommended to always use HDFS as running Spark on DAS-5 generates a lot of small files. While the main file-server is mounted on every compute node of the cluster, it is still recommended to put any necessary files for the Spark job in HDFS.

Check the HDFS NameNode's hostname by going into the hadoop frameworks and checking the **core-site.xml** file:
```shell
cd das-bigdata-deployment/frameworks/hadoop-<version>/etc/hadoop
vim hdfs-site.xml
```

Look for **fs.defaultFS** or **fs.default.name** property, which should have a value similar to **hdfs://<namenode-hostname>:<port>**

ex) hdfs://node339.ib.cluster:9000

Once you have confirmed the HDFS hostname add the necessary files to HDFS:

```shell
cd das-bigdata-deployment

./frameworks/hadoop-3.2.4/bin/hadoop fs -mkdir -p hdfs://node339.ib.cluster:9000/jars
./frameworks/hadoop-3.2.4/bin/hadoop fs -rm hdfs://node339.ib.cluster:9000/jars/SimpleSpark.jar
./frameworks/hadoop-3.2.4/bin/hadoop fs -put /home/<user>/SimpleSpark.jar hdfs://node339.ib.cluster:9000/jars/SimpleSpark.jar
./frameworks/hadoop-3.2.4/bin/hadoop fs -ls hdfs://node339.ib.cluster:9000/jars/SimpleSpark.jar
```

### Running Spark
After deploying both Hadoop and Spark cluster, connect to the master compute node via SSH and submit the Spark application:

Note:
- can't have more than 4 cores per executor instance
- configuration values here will override the configuration values in specified in the JAR file

```
cd das-bigdata-deployment
./frameworks/spark-3.2.4/bin/spark-submit --class org.example.Main --num-executors 4 --executor-memory 30g --executor-cores 1 hdfs://node339.ib.cluster:9000/jars/SimpleSpark.jar hdfs://node339.ib.cluster:9000/input/wordcount.txt
```
