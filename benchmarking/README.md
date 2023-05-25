# Benchmarking Module

This module provides all the tools we use for benchmarking the plugin. The documentation for this module is still very incomplete as it is still under active experimentation. These tools are also potentially useful for
anyone who wants to utilize this plugin as a way to evaluate its performance. Currently, it consists of:

### DAS scripts

We used the Distributed ASCI Supercomputer (DAS) system to benchmark our plugin. Information can be found [here](https://asci.tudelft.nl/project-das/).

The DAS scripts are Python scripts that submit a slot allocation request on the DAS, whilst configuring the spark environment.

It is based on the work of Tim Hegeman, Chris Lemaire, and Laurens Versluis. You can find the forked source repo [here](https://github.com/lfdversluis/das-bigdata-deployment).

