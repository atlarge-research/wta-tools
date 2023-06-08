#!/usr/bin/env python2
# This script (The DAS configuration script) is based on the work of Tim Hegeman, Chris Lemaire, and Laurens Versluis
# The github repo for the original script: https://github.com/lfdversluis/das-bigdata-deployment

from frameworkmanager import Framework, FrameworkVersion, FrameworkRegistry, FrameworkManager, get_framework_registry
import hadoop, spark

