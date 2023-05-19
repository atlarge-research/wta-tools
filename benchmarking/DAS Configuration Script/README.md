# Deployment scripts for big data frameworks on DAS-5

These are the scripts I use to download, configure, and deploy several big data frameworks (YARN/MapReduce, Spark) and related systems (HDFS, ZooKeeper, InfluxDB).

Warning: I have not extensively tested these scripts for different users. The scripts assume ownership of the `/local/$USER/` directory on every node used in a deployment. In particular, the scripts will wipe the `/local/$USER/{hadoop,spark,zookeeper,influxdb}` directory before the respective application is deployed.

## Installation

1. Git clone this repository to your home directory on DAS-5. The resulting directory will be referred to as `$DEPLOYER_HOME` throughout this manual.

Optional if space in your home directory is limited (deployments are likely to generate gigabytes of logs over time):

2. Create a directory in your scratch folder for the big data frameworks and configuration files, e.g., `/var/scratch/$USER/big-data-frameworks`.

3. Create a symlink in `$DEPLOYER_HOME` called `frameworks` pointing at the directory you created at point 2.

The deployment scripts can now be used to deploy any of the included frameworks.

## Reserving nodes on DAS-5

The deployer can create a new reservation via `preserve` or you may use existing reservations. To create a reservation run:

```bash
$DEPLOYER_HOME/deployer preserve create-reservation -q -t "$TIMEOUT" $MACHINES
```

where `$TIMEOUT` should be the duration of the reservation in `hh:mm:ss` format and `$MACHINES` should be the number of nodes to reserve. The output includes the ID of your reservation.

Use the following (substituting your reservation ID) to check the status of your reservation:

```bash
$DEPLOYER_HOME/deployer preserve fetch-reservation $RESERVATION_ID
```

## Deploying frameworks

To get a list of supported frameworks and versions, run:

```bash
$DEPLOYER_HOME/deployer list-frameworks --versions
```

Before a framework can be deployed, it must be "installed". This only needs to be done once. After installing, the framework can be repeatedly deployed. In the following command, substitute a framework name and version as output by the `deployer list-frameworks` command.

```bash
$DEPLOYER_HOME/deployer install $FRAMEWORK $VERSION
```

To deploy a framework, use the `deployer deploy -h` command for help, or use one of the following standard deployments.

### Deploying Hadoop

To deploy Hadoop (HDFS and YARN) with sensible defaults, run the following command (substituting your reservation ID):

```bash
./deployer deploy --preserve-id $RESERVATION_ID -s env/das5-hadoop.settings hadoop 2.6.0
```

If you do not need HDFS or YARN append the `hdfs_enable=false` or `yarn_enable=false` options, respectively, to the above comand.

Note: the deployer launches master processes on the first machine in the reservation (as indicated in the output of the deploy command). To connect to HDFS or YARN, first connect to that machine via SSH and then use Hadoop from the `$DEPLOYER/frameworks/hadoop-2.6.0` directory.

### Deploying Spark

To deploy Spark with sensible defaults, run the following command (substituting your reservation ID):

```bash
./deployer deploy --preserve-id $RESERVATION_ID -s env/das5-spark.settings spark 2.4.0
```

To connect to Spark using a shell, first connect to the application master via SSH, then run `$DEPLOYER_HOME/frameworks/spark-2.4.0/bin/spark-shell` to open a Spark session connected to the cluster.
