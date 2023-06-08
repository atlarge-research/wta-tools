## This script (wta-tools) is based on the work of Laurens Versluis[@lfdversluis](https://github.com/lfdversluis) and [@JaroAmsterdam](https://github.com/JaroAmsterdam)
## The github repo for the original script: https://github.com/atlarge-research/wta-tools

# wta-tools
This directory provides the tool for validating the Spark plugin parquet output in the WTA format. You can find the WTA format schema at: https://wta.atlarge-research.com/traceformat.html

## Running wta-tools
The instructions below are meant for running tool in any environment.

### Requirements
Ensure that you have the following tools installed and environment path variables set:
- Python 3.6 or above

Furthermore, you need to have the WTA trace parquet files as the following directory structure:

```
<dir_name>
    workload
        schema-1.0
            generic_information.json
    workflows
        schema-1.0
            workflow.parquet
    tasks
        schema-1.0
            task.parquet
    resources
        schema-1.0
            resource.parquet    
```

### Download the submodule
Download this submodule separately from the repo using the instructions specified in the [README](../README.md). 

### Create virtual environment and install dependencies
This script makes use of third-party libraries to run the script, thus a virtual environment is needed. These dependencies are specified in the `requirements.txt` file. To create a virtual environment, run the following command:

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

<dir_name> is the directory of the WTA trace parquet files.
