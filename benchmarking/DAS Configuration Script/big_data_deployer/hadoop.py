#!/usr/bin/env python2

from __future__ import print_function
from .frameworkmanager import Framework, FrameworkVersion, FrameworkRegistry, get_framework_registry
from . import util
import glob
import os.path
import re

_SETTING_JAVA_HOME = "java_home"
_SETTING_HDFS_ENABLE = "hdfs_enable"
_SETTING_YARN_ENABLE = "yarn_enable"
_SETTING_YARN_MB = "yarn_memory_mb"
_SETTING_YARN_CORES = "yarn_cores"
_SETTING_LOG_AGGREGATION = "log_aggregation"
_SETTING_USERLOGS_DIR = "userlogs_dir"
_ALL_SETTINGS = [
    (_SETTING_JAVA_HOME, "value of JAVA_HOME to deploy Hadoop with"),
    (_SETTING_HDFS_ENABLE, "deploy Hadoop's HDFS"),
    (_SETTING_YARN_ENABLE, "deploy Hadoop's YARN"),
    (_SETTING_YARN_MB, "memory available per node to YARN in MB"),
    (_SETTING_YARN_CORES, "cores available per node to YARN"),
    (_SETTING_LOG_AGGREGATION, "enable YARN log aggregation"),
    (_SETTING_USERLOGS_DIR, "directory to store YARN application logs")
]

_DEFAULT_HDFS_ENABLE = True
_DEFAULT_YARN_ENABLE = True
_DEFAULT_YARN_MB = 4096
_DEFAULT_YARN_CORES = 8
_DEFAULT_LOG_AGGREGATION = False
_DEFAULT_USERLOGS_DIR = "${yarn.log.dir}/userlogs"

class HadoopFrameworkVersion(FrameworkVersion):
    def __init__(self, version, archive_url, archive_extension, archive_root_dir, template_dir):
        super(HadoopFrameworkVersion, self).__init__(version, archive_url, archive_extension, archive_root_dir)
        self.__template_dir = template_dir

    @property
    def template_dir(self):
        return self.__template_dir

class HadoopFramework(Framework):
    def __init__(self):
        super(HadoopFramework, self).__init__("hadoop", "Hadoop")

    def deploy(self, hadoop_home, framework_version, machines, settings, log_fn=util.log):
        """Deploys Hadoop to a given set of workers and a master node."""
        if len(machines) < 2:
            raise util.InvalidSetupError("Hadoop requires at least two machines: a master and at least one worker.")

        # Extract settings
        hdfs_enable_str = str(settings.pop(_SETTING_HDFS_ENABLE, _DEFAULT_HDFS_ENABLE)).lower()
        hdfs_enable = hdfs_enable_str in ['true', 't', 'yes', 'y', '1']
        yarn_enable_str = str(settings.pop(_SETTING_YARN_ENABLE, _DEFAULT_YARN_ENABLE)).lower()
        yarn_enable = yarn_enable_str in ['true', 't', 'yes', 'y', '1']
        yarn_mb = settings.pop(_SETTING_YARN_MB, _DEFAULT_YARN_MB)
        yarn_cores = settings.pop(_SETTING_YARN_CORES, _DEFAULT_YARN_CORES)
        java_home = settings.pop(_SETTING_JAVA_HOME)
        log_aggregation_str = str(settings.pop(_SETTING_LOG_AGGREGATION, _DEFAULT_LOG_AGGREGATION)).lower()
        log_aggregation = log_aggregation_str in ['true', 't', 'yes', 'y', '1']
        userlogs_dir = settings.pop(_SETTING_USERLOGS_DIR, _DEFAULT_USERLOGS_DIR)
        if len(settings) > 0:
            raise util.InvalidSetupError("Found unknown settings for Hadoop: '%s'" % "','".join(settings.keys()))
        if not hdfs_enable and not yarn_enable:
            raise util.InvalidSetupError("At least one of HDFS and YARN must be deployed.")

        # Select master and workers
        master = machines[0]
        workers = machines[1:]
        log_fn(0, "Deploying Hadoop master \"%s\", with %d workers." % (master, len(workers)))

        # Ensure that HADOOP_HOME is an absolute path
        hadoop_home = os.path.realpath(hadoop_home)

        # Generate configuration files using the included templates
        template_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "conf", "hadoop", framework_version.template_dir)
        config_dir = os.path.join(hadoop_home, "etc", "hadoop")
        substitutions = {
            "__USER__": os.environ["USER"],
            "__MASTER__": master,
            "__YARN_MB__": str(yarn_mb),
            "__YARN_CORES__": str(yarn_cores),
            "__LOG_AGGREGATION__": "true" if log_aggregation else "false",
            "__USERLOGS_DIR__": userlogs_dir
        }
        if java_home:
            substitutions["${JAVA_HOME}"] = java_home
        substitutions_pattern = re.compile("|".join([re.escape(k) for k in substitutions.keys()]))
        # Iterate over template files and apply substitutions
        log_fn(1, "Generating configuration files...")
        for template_file in glob.glob(os.path.join(template_dir, "*.template")):
            template_filename = os.path.basename(template_file)[:-len(".template")]
            log_fn(2, "Generating file \"%s\"..." % template_filename)
            with open(template_file, "r") as template_in, open(os.path.join(config_dir, template_filename), "w") as config_out:
                for line in template_in:
                    print(substitutions_pattern.sub(lambda m: substitutions[m.group(0)], line.rstrip()), file=config_out)
        log_fn(2, "Generating file \"masters\"...")
        with open(os.path.join(config_dir, "masters"), "w") as masters_file:
            print(master, file=masters_file)
        log_fn(2, "Generating file \"slaves\"...")
        with open(os.path.join(config_dir, "slaves"), "w") as slaves_file:
            for worker in workers:
                print(worker, file=slaves_file)
        log_fn(2, "Configuration files generated.")

        # Clean up previous Hadoop deployments
        log_fn(1, "Creating a clean environment on the master and workers...")
        local_hadoop_dir = "/local/%s/hadoop/" % substitutions["__USER__"]
        log_fn(2, "Purging \"%s\" on master..." % local_hadoop_dir)
        util.execute_command_quietly(["ssh", master, 'rm -rf "%s"' % local_hadoop_dir])
        log_fn(2, "Purging \"%s\" on workers..." % local_hadoop_dir)
        for worker in workers:
            util.execute_command_quietly(['ssh', worker, 'rm -rf "%s"' % local_hadoop_dir])
        log_fn(2, "Creating directory structure on master ({})...".format(master))
        util.execute_command_quietly(['ssh', master, 'mkdir -p "%s"' % local_hadoop_dir])
        log_fn(2, "Creating directory structure on workers...")
        for worker in workers:
            util.execute_command_quietly(['ssh', worker, 'mkdir -p "%s/tmp" "%s/datanode"' % (local_hadoop_dir, local_hadoop_dir)])
        log_fn(2, "Clean environment set up.")

        # Start HDFS
        if hdfs_enable:
            log_fn(1, "Deploying HDFS...")
            log_fn(2, "Formatting namenode...")
            util.execute_command_quietly(['ssh', master, '"%s/bin/hadoop" namenode -format' % hadoop_home])
            log_fn(2, "Starting HDFS...")
            util.execute_command_quietly(['ssh', master, '"%s/sbin/start-dfs.sh"' % hadoop_home])

        # Start YARN
        if yarn_enable:
            log_fn(1, "Deploying YARN...")
            util.execute_command_quietly(['ssh', master, '"%s/sbin/start-yarn.sh"' % hadoop_home])

        log_fn(1, "Hadoop cluster deployed.")

    def get_supported_deployment_settings(self, framework_version):
        return _ALL_SETTINGS

get_framework_registry().register_framework(HadoopFramework())
get_framework_registry().framework("hadoop").add_version(HadoopFrameworkVersion("2.6.0", "https://archive.apache.org/dist/hadoop/core/hadoop-2.6.0/hadoop-2.6.0.tar.gz", "tar.gz", "hadoop-2.6.0", "2.6.x"))
get_framework_registry().framework("hadoop").add_version(HadoopFrameworkVersion("2.7.7", "https://archive.apache.org/dist/hadoop/core/hadoop-2.7.7/hadoop-2.7.7.tar.gz", "tar.gz", "hadoop-2.7.7", "2.6.x"))
get_framework_registry().framework("hadoop").add_version(HadoopFrameworkVersion("3.2.2", "https://archive.apache.org/dist/hadoop/core/hadoop-3.2.2/hadoop-3.2.2.tar.gz", "tar.gz", "hadoop-3.2.2", "2.6.x"))
