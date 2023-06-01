# WTA Generation Toolkit

## Installation and Usage
- [Spark Plugin](/adapter/spark/README.md#installation-and-usage)

## Configuration
The converter expects some user-defined configuration. This is done via a JSON file.
An example of a valid configuration can be seen here:

```json
{
  "authors": ["John Doe", "Jane Doe"],
  "domain": "Scientific",
  "description": "Processing data for scientific research purposes",
  "events": {
    "f1": "v1",
    "f2": "v2"
  },
  "logLevel": "INFO",
  "outputPath": "wta-output",
  "resourcePingInterval": 1000,
  "executorSynchronizationInterval": 2000
}

```

### Configuration Description
Below is an explanation of each field and their expected types.

| Field                           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         Description |    Expected Type     |
|---------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:--------------------:|
| authors                         |                                                                                                                                                                                                                                                                                                                                                                                         The author(s) of the trace. Even if there is 1 author, they must still be specified in an array. This is a mandatory field. |   `ARRAY[STRING]`    |
| domain                          |                                                                                                                                                                                                                                                                                                                                                The domain that the job corresponds to. This must be either 'Biomedical', 'Engineering', 'Industrial' or 'Scientific'. This is a case-sensitive and mandatory field. |       `STRING`       |
| description                     |                                                                                                                                                                                                                                                                                                                                                                                                                                     The description of the job. This field is highly recommended but not mandatory. |       `STRING`       |
| events                          |                                                                                                                                                                                                                                                                                                                                                                                          Any additional information that needs to be passed that is not present in the trace format. This is a non-mandatory field. | `MAP[STRING,STRING]` |
| logLevel                        |                                                                                                                                                                                                                                                                                                                            The granularity of which the events/errors should be logged. This is one of `OFF`,`ALL`,`TRACE`,`DEBUG`,`FATAL`,`WARN`,`INFO` or`ERROR`. This field is case-sensitive and non-mandatory. |       `STRING`       |
| outputPath                      |                                                                                                                                                                                                                                                                                                                                                                                                                                                   The output path of the generated trace. This is a mandatory field |       `STRING`       |
| resourcePingInterval            |                                                                         How often the resources are pinged for metrics in milliseconds. By default this is set to 1000 and it is encouraged that the user does not modify this unless they know exactly what they are doing, as modifying this in a naive manner can introduce unforeseen effects. If this parameter is too large, metrics will not be captured for tasks that have a lifespan shorter than the respective interval. This is a non-mandatory field. |       `INT32`        |
| executorSynchronizationInterval | How often executors/slaves send their captured resource metrics to the driver/master in milliseconds. By default this is set to 2000 and it is encouraged that the user does not modify this unless they know exactly what they are doing, as modifying this in a naive manner can introduce unforeseen effects. If resources are pinged and a task subsequently ends before a buffer synchronization tick, the respective resources will not be included in the aggregated metrics. This is a non-mandatory field. |       `INT32`        |


### Configuration per Application
[Spark Plugin](/adapter/spark/README.md#configuration)



## Authors and Acknowledgement

### Developers 12A
- [Henry Page](https://gitlab.ewi.tudelft.nl/hpage)
- [Pil Kyu Cho](https://gitlab.ewi.tudelft.nl/pcho)
- [Lohithsai Yadala Chanchu](https://gitlab.ewi.tudelft.nl/lyadalachanchu)
- [Atour Mousavi Gourabi ](https://gitlab.ewi.tudelft.nl/amousavigourab)
- [Tianchen Qu](https://gitlab.ewi.tudelft.nl/tqu)

### Others
- **Teaching Assistant:** [Timur Oberhuber](https://gitlab.ewi.tudelft.nl/toberhuber)
- **Supervisor:** Johan Pouwelse
- **Client:** Laurens Versluis on behalf of ASML

## License
This project is licensed under the APACHE-2.0 license.
For more information see [LICENSE](LICENSE) and [NOTICE](NOTICE).
