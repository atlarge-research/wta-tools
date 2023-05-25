# WTA Spark Plugin

## Installation and Usage
- Clone the repository
- Run `mvn clean install` in the source root.
- Copy the resulting jar file from `adapter/spark/target`.
- Run the following command in the directory where the jar file is located:
```bash
spark-submit --class com.asml.apa.wta.spark.App
--master local[1] <plugin_jar_location> <config.json_location> <directory_for_outputted_parquet> <file_to_be_processed>
```
- The parquet files should now be in the <directory_for_outputted_parquet>.

### Logging
It is important that when using the `core` module to build adapter layers, `Log4j2Configuration#setUpLoggingConfig`
is invoked early to set the user defined part of the logging configuration.
As of now, the user can define one part of the logging configuration using `core` logging, the log level. This
is set to `ERROR` by default, but the user can exert full control over this using the `config.json` file.

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
For open source projects, say how it is licensed.
