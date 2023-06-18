# WTA Generation Toolkit

## Installation and Usage
- [Spark Plugin](/adapter/spark/README.md#installation-and-usage)

### Deployment
The applications use optional packages on Linux to gather more information and generate more complete traces.
The packages that are used for this purpose are `sysstat`, `dstat`, and `perf`. To install them, you need to install
their corresponding packages, or alternatively layer the provided `Dockerfiles` over your own Docker images to deploy
in containerised environments.

#### Dockerfiles
If you are running in a containerised environment, you might want to layer our provided `Dockerfile` on top of your
image. `Dockerfiles` for this purpose are provided for Alpine, CentOS, Debian, and Ubuntu. The `Dockerfiles` can be
found in `submodules/dockerfiles`, under their respective distro names (`alpine`, `centos`, `debian`, `ubuntu`).
To layer them on top of existing images, please run the following command, after setting the base image and generated
image tags, and defining the Linux distro to use the `Dockerfile` of.

```shell
docker build --build-arg base_image=[BASE IMAGE TAG] -t [IMAGE TAG] path/to/repository/submodules/dockerfiles/[DISTRO]
```

Note: it is important to note that for `perf` to be installed correctly, the host machine will need to run a kernel
that is compatible with the virtualised OS. This means that when you use hosts for your jobs that run
`5.4.0-149-generic`, you cannot use an Ubuntu Jammy image when running Ubuntu, as that OS is incompatible with this
kernel. If the pool of hosts is diverse enough for this not to be a viable (i.e., multiple incompatible kernel
versions are being  used among the hosts), you might want to consider removing the `perf` installation from the
`Dockerfile` you layer on top of your image.

To allow the application to make use of `perf` commands, it is important the relevant `perf` commands are available
to the application. This means the application will either have to be run with `sudo`, or `perf_even_paranoia`
needs to be set to 0. Even when layering one of the provided `Dockerfiles`, this will still have to be set. This can
be done by running the following command:

```shell
sysctl -w kernel.perf_event_paranoid=0
```

## Configuration
The converter expects some user-defined configuration. This is done via a JSON file.
An example of a valid configuration can be seen here:

```json
{
  "authors": ["John Doe", "Jane Doe"],
  "domain": "Scientific",
  "description": "Processing data for scientific research purposes",
  "logLevel": "INFO",
  "outputPath": "wta-output",
  "resourcePingInterval": 500,
  "executorSynchronizationInterval": -1
}

```

### Configuration Description
Below is an explanation of each field and their expected types. The default values for certain fields were determined by running

| Field                           |                                                                                                                                                                                                                                                                                                                                                                                                                                                           Description |  Expected Type  | Mandatory          |
|---------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:---------------:|--------------------|
| authors                         |                                                                                                                                                                                                                                                                                                                                                                      The author(s) of the trace. Even if there is 1 author, they must still be specified in an array. | `ARRAY[STRING]` | :heavy_check_mark: |
| domain                          |                                                                                                                                                                                                                                                                                                                The domain that the job corresponds to. This must be either 'Biomedical', 'Engineering', 'Industrial' or 'Scientific'. This is a case-sensitive field. |    `STRING`     | :heavy_check_mark: |
| description                     |                                                                                                                                                                                                                                                                                                                                                                                                                                           The description of the job. |    `STRING`     |                    |
| logLevel                        |                                                                                                                                                                                                                                                                                                The granularity of which the events/errors should be logged. This is one of `OFF`,`ALL`,`TRACE`,`DEBUG`,`FATAL`,`WARN`,`INFO` or`ERROR`. This field is case-sensitive. |    `STRING`     |                    |
| outputPath                      |                                                                                                                                                                                                                                                                                                                                                                                                                               The output path of the generated trace. |    `STRING`     | :heavy_check_mark: |
| resourcePingInterval            |                                                       How often the resources are pinged for metrics in milliseconds. By default this is set to 500 and it is encouraged that the user does not modify this unless they know exactly what they are doing, as modifying this in a naive manner can introduce unforeseen effects. If this parameter is too large, metrics will not be captured for executors that have a lifespan shorter than the respective interval. |     `INT32`     |                    |
| executorSynchronizationInterval | How often executors/slaves send their captured resource metrics to the driver/master in milliseconds. By default this is set to -1.  If the resources are pinged and the executor subsequently ends before a buffer synchronization tick, the respective resources will not be included in the aggregated metrics on the driver side. If this value is non-positive, resource information will be sent immediately after it is collected and it will not be buffered. |     `INT32`     |                    |


### Configuration per Application
[Spark Plugin](/adapter/spark/README.md#configuration)


## Developer Guidelines

### General Architecture (TL;DR)

The structure of this framework is split into two modules. Namely, `core` and `adapter`.

#### Core
The `core` module contains logic and information which is agnostic to the application that is being traced. This includes, but is not limited to:
- DTOs (Data Transfer Object) which are used to represent the events and resources that are captured.
- Exceptions which can be generalized to any application.
- I/O logic which is used to convert the execution trace to Parquet.
- WTA models.
- Application agnostic resource utilisation metrics including the extraction engine.
- General utility classes.

More information can be found in the [core](./core/README.md) module.

#### Adapter
The `adapter` module contains logic and information which is specific to the application that is being traced. This includes, but is not limited to:
- Application specific functionality (e.g. listening to Spark events).
- Application specific information that needs to be tracked (e.g. `executorId`).
- Additional DTOs that are specific to the application.
- Anything that extends the functionality of classes in the `core` module, to make it tailored to the application.

More information can be found in the [adapter](./adapter/README.md) module.

Additional modules can be found in the submodules directory. Currently, they include the following things
- Benchmarking
  - Contains scripts to benchmark the performance of the framework against the Distributed ASCI Supercomputer 5 (DAS-5). More information can be found in the [here](./submodules/benchmarking/README.md).
- wta-tools
  - Instructions to use validation scripts. More information can be found in [here](./submodules/wta-tools/README.md).

#### Maven Verification and Deployment
To run the linters and other verification goals on the code, you need to run `mvn clean verify`.
In order to generate the site, you need to run `mvn site`. If you want the site to contain information
on testing, such as code coverage, you need to have JaCoCo reports present on your machine. A simple way
to ensure that this works is by running `mvn clean verify site`. The site will be located in the `target/site`
directory in every module by default.

By default, the `mvn site` phase will generate separate sites for all the modules. To compile all the sites,
you need to run `mvn site:stage` after running the site phase. This will compile all the sites in the
`target/staging` directory in the top level project.


## Authors and Acknowledgement

### Developers 12A
- [Henry Page](https://gitlab.ewi.tudelft.nl/hpage)
- [Pil Kyu Cho](https://gitlab.ewi.tudelft.nl/pcho)
- [Lohithsai Yadala Chanchu](https://gitlab.ewi.tudelft.nl/lyadalachanchu)
- [Atour Mousavi Gourabi](https://gitlab.ewi.tudelft.nl/amousavigourab)
- [Tianchen Qu](https://gitlab.ewi.tudelft.nl/tqu)

### Others
- **Teaching Assistant:** [Timur Oberhuber](https://gitlab.ewi.tudelft.nl/toberhuber)
- **Supervisor:** Johan Pouwelse
- **Client:** Laurens Versluis on behalf of ASML

## License
This project is licensed under the APACHE-2.0 license.
For more information see [LICENSE](LICENSE) and [NOTICE](NOTICE).
