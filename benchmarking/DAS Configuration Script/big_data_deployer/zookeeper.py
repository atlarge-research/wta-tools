#!/usr/bin/env python2
# This script (The DAS configuration script) is based on the work of Tim Hegeman, Chris Lemaire, and Laurens Versluis 
# The github repo for the original script: https://github.com/lfdversluis/das-bigdata-deployment
from __future__ import print_function
from .frameworkmanager import Framework, FrameworkVersion, FrameworkRegistry, get_framework_registry
from . import util
import glob
import os.path
import re

class ZookeeperFrameworkVersion(FrameworkVersion):
    def __init__(self, version, archive_url, archive_extension, archive_root_dir, template_dir):
        super(ZookeeperFrameworkVersion, self).__init__(version, archive_url, archive_extension, archive_root_dir)
        self.__template_dir = template_dir

    @property
    def template_dir(self):
        return self.__template_dir

class ZookeeperFramework(Framework):
    def __init__(self):
        super(ZookeeperFramework, self).__init__("zookeeper", "ZooKeeper")

    def deploy(self, zookeeper_home, framework_version, machines, settings, log_fn=util.log):
        """Deploys ZooKeeper to a given master node."""
        if len(machines) < 1:
            raise util.InvalidSetupError("ZooKeeper requires at least one machine to run on.")

        master = machines[0]
        log_fn(0, "Selected ZooKeeper machine \"%s\"." % master)

        # Ensure that ZOOKEEPER_HOME is an absolute path
        zookeeper_home = os.path.realpath(zookeeper_home)

        # ZooKeeper currently has no settings
        if len(settings) > 0:
            raise util.InvalidSetupError("Found unknown settings for ZooKeeper: '%s'" % "','".join(settings.keys()))

        # Generate configuration files using the included templates
        template_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "conf", "zookeeper", framework_version.template_dir)
        config_dir = os.path.join(zookeeper_home, "conf")
        substitutions = {
            "__USER__": os.environ["USER"],
        }
        substitutions_pattern = re.compile("|".join([re.escape(k) for k in substitutions.keys()]))
        # Iterate over template files and apply substitutions
        log_fn(1, "Generating configuration files...")
        for template_file in glob.glob(os.path.join(template_dir, "*.template")):
            template_filename = os.path.basename(template_file)[:-len(".template")]
            log_fn(2, "Generating file \"%s\"..." % template_filename)
            with open(template_file, "r") as template_in, open(os.path.join(config_dir, template_filename), "w") as config_out:
                for line in template_in:
                    print(substitutions_pattern.sub(lambda m: substitutions[m.group(0)], line.rstrip()), file=config_out)
        log_fn(2, "Configuration files generated.")

        # Clean up previous ZooKeeper deployments
        log_fn(1, "Creating a clean environment on the ZooKeeper machine...")
        local_zookeeper_dir = "/local/%s/zookeeper/" % substitutions["__USER__"]
        log_fn(2, "Purging \"%s\"..." % local_zookeeper_dir)
        util.execute_command_quietly(["ssh", master, 'rm -rf "%s"' % local_zookeeper_dir])
        log_fn(2, "Creating directory structure...")
        util.execute_command_quietly(['ssh', master, 'mkdir -p "%s"' % local_zookeeper_dir])
        log_fn(2, "Clean environment set up.")

        # Start YARN
        log_fn(1, "Deploying ZooKeeper...")
        util.execute_command_quietly(['ssh', master, '"%s/bin/zkServer.sh" start' % zookeeper_home])

        log_fn(1, 'ZooKeeper is now listening on "%s:2181".' % master)

    def get_supported_deployment_settings(self, framework_version):
        return []

get_framework_registry().register_framework(ZookeeperFramework())
get_framework_registry().framework("zookeeper").add_version(ZookeeperFrameworkVersion("3.7.1", "https://archive.apache.org/dist/zookeeper/zookeeper-3.7.1/zookeeper-3.7.1.tar.gz", "tar.gz", "zookeeper-3.7.1", "3.7.x"))
