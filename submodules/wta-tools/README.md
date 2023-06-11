## [wta-tools](https://github.com/atlarge-research/wta-tools) is the work of [Laurens Versluis](https://github.com/lfdversluis) and [Jaro Bosch](https://github.com/JaroAmsterdam)

# wta-tools
wta-tools validates the Spark plugin Parquet output in the WTA format. You can find the WTA trace schema [here](https://wta.atlarge-research.com/traceformat.html)

Note that the WTA trace schema from the above link might be outdated. Should there be any conflicts, make use of the schema from the script's [GitHub repository](https://github.com/atlarge-research/wta-tools).

## Running wta-tools
The instructions below are meant for running the tool in any environment.

### Requirements
Ensure that you have the following tools installed and environment path variables set:
- Python 3.6 or above

Furthermore, you need to have the WTA trace Parquet files as the following directory structure:

```
<dir_name>
    workload
        schema-1.0
            generic_information.json
    workflows
        schema-1.0
            workflows.parquet
    tasks
        schema-1.0
            tasks.parquet
    resources
        schema-1.0
            resources.parquet    
```
### Download script
Download the script from the github repo:
```
git clone https://github.com/atlarge-research/wta-tools
```

### Create virtual environment and install dependencies
The script makes use of third-party libraries, thus a virtual environment is needed. To create a virtual environment, run the following command:

```bash
cd wta-tools
python3 -m venv <name_of_your_virtual_env>
```

Then activate the virtual environment.

For Linux systems:
```bash
source venv/bin/activate
```

For Windows system:
```bash
source venv/Scripts/activate
```

Create `requirements.txt` file in the root directory and add the following dependencies:
```
numpy==1.24.3
pandas==2.0.1
pyarrow==12.0.0
pyspark==3.2.4
toposort==1.10
```

Verify that you are inside the virtual environment and install the dependencies:

```bash
pip3 install -r requirements.txt
```

### Run the script
Once all the dependencies are installed, you can run the script with the following command:

```bash
cd wta-tools
python parse_scripts/validate_parquet_files.py <dir_name>
```

<dir_name> is the directory of the WTA trace Parquet files.
