# CHANGELOG
For the most part, since we are pre-release this will serve no real purpose, other than accountability. Initial Release will correspond to minimum viable product.

## [Unreleased]
### Added
- Basic pipeline setup [@lyadalachanchu](https://gitlab.ewi.tudelft.nl/lyadalachanchu)
- Initial project setup & pipeline fix [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Augmented the pipeline [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Added basic config file & read utils [@lyadalachanchu](https://gitlab.ewi.tudelft.nl/lyadalachanchu)
- Added plugin initialisation support [@hpage](https://gitlab.ewi.tudelft.nl/hpage)
- Fixed the pipeline's testing stage, added checkstyle, and incorporated basic coverage reporting to GitLab CI/CD in the pipeline [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Created basic skeleton of Workflow object [@lyadalachanchu](https://gitlab.ewi.tudelft.nl/lyadalachanchu)
- Created the benchmarking module for performance evaluation of SparkMeasure plugin [@pcho](https://gitlab.ewi.tudelft.nl/pcho)
- Implemented basic methods of CollectorInterface [@hpage](https://gitlab.ewi.tudelft.nl/hpage)
- Creates report artefacts in the pipeline and removed a job [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Created basic skeleton of Task object [@lyadalachanchu](https://gitlab.ewi.tudelft.nl/lyadalachanchu)
- Patched report artefacts in the pipeline [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Created basic skeleton of Resource and Workload objects [@lyadalachanchu](https://gitlab.ewi.tudelft.nl/lyadalachanchu)
- Created the Spark datasource class to access Spark metrics [@pcho](https://gitlab.ewi.tudelft.nl/pcho)
- Set up basic streaming infrastructure [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Added a build dependency to PMD [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Added events field for resource object to be configurable [@lyadalachanchu](https://gitlab.ewi.tudelft.nl/lyadalachanchu)
- Standardize schema version across all objects [@lyadalachanchu](https://gitlab.ewi.tudelft.nl/lyadalachanchu)
- Updated TaskLevelLIstener to collect as much information from Spark for the Task object [@hpage](https://gitlab.ewi.tudelft.nl/hpage)
- Improved PMD in the pipeline [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Includes automatic serialization to the streaming package [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Update WtaUtils file to parse for user logging preferences [@pcho](https://gitlab.ewi.tudelft.nl/pcho)
- Created a logging class to log on both console and to log file [@pcho](https://gitlab.ewi.tudelft.nl/pcho)
- Updates the Spark dependency to 3.2.4 [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Utilise Spark API to collect information about workflow and workload [@hpage](https://gitlab.ewi.tudelft.nl/hpage)
- Moves to using slf4j as logging facade over directly using log4j2 [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Add DAS configuration script to benchmark [@tqu](https://gitlab.ewi.tudelft.nl/tqu)[@pcho](https://gitlab.ewi.tudelft.nl/pcho)
- Simplified the use of the streaming packages [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Created utility class for writing WTA trace to parquet files [@tqu](https://gitlab.ewi.tudelft.nl/tqu)
- Added Java MX Bean data source for resource metrics [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Created README documentation for all modules [@tqu](https://gitlab.ewi.tudelft.nl/tqu) [@hpage](https://gitlab.ewi.tudelft.nl/hpage)
- Add maven-assembly plugin to create fat jar  [@pcho](https://gitlab.ewi.tudelft.nl/pcho) [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Added license information to the project [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Added iostat datasource [@lyadalachanchu](https://gitlab.ewi.tudelft.nl/lyadalachanchu)
- Created custom images for e2e and integration testing [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Wired e2e testing into the pipeline [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Make the WtaPlugin class dictate the Spark adapter lifecycle [@pcho](https://gitlab.ewi.tudelft.nl/pcho)
- Added option to choose between stage and task level metrics [@lyadalachanchu](https://gitlab.ewi.tudelft.nl/lyadalachanchu)

### Changed
- Refactored listeners into a generic interface to reduce code duplication and increase cc [@hpage](https://gitlab.ewi.tudelft.nl/hpage)
- Merged the JaCoCo coverage metrics for integration and unit tests [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Decreased JAR size by around 75% [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Modified the config format [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Modified the EndToEnd class to inject the WtaPlugin class into a Spark context rather the Spark listener interface by itself [@pcho](https://gitlab.ewi.tudelft.nl/pcho)

### Fixed
- Fixed the slf4j logging module to allow use in testing and `adapter/spark` [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Fixed the Parquet and JSON writer to use snake case to adhere to the WTA [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Fixed the DAS5 framework versions to download [@pcho](https://gitlab.ewi.tudelft.nl/pcho)
- Fixed the format of the outputted files [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)

### Deprecated

### Removed
- Removed Spotbugs SAST from the pipeline [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
- Removed mutation testing from the pipeline [@amousavigourab](https://gitlab.ewi.tudelft.nl/amousavigourab)
