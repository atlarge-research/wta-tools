# Submodules
This directory provides all the tools that are used with the Spark plugin.

## Directory structure
There are currently 2 submodules in this directory:
- **benchmarking**
- **wta-tools**

## benchmarking
Provides tools used for benchmarking the Spark plugin. This is *not* packaged with the Spark plugin.

## wta-tools
Tool to validate the generated Spark plugin parquet output in the WTA format.

## Downloading the submodules
Some of the submodules can be used independently of the plugin. To download submodules separately and not the entire repository, follow the instructions below:

Create a new directory for the repository and navigate into it:
```bash
mkdir my_project
cd my_project
```

Initialize an empty git repository and add the URL of this remote repository:
```bash
git init
git remote add -f origin https://github.com/user/repo.git
```

Enable sparse checkout:
```bash
git config core.sparseCheckout true
```

Specify the subdirectory you want to checkout. In this example, assume you want to download the `wta-tools` subdirectory:
```bash
echo "wta-tools/*" > .git/info/sparse-checkout
```

Finally, pull content from remote repo:
```bash
git pull origin main
```