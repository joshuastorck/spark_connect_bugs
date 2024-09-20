
## Overview

This is a simple test case that demonstrates how `withColumnRenamed` does not work correctly when using Databricks connect.

## Requirements

Poetry 1.8.3
Python 3.10

JAVA_HOME must be in the environment or the `java` binary must be available on the path

## Steps to reproduce

1. Run download_spark.sh
2. Run poetry install
3. Run poetry run python test.py, which should exhibit the correct behavior and displays a column named 'val2'
4. Run poetry install --group dev databricks-connect
5. Run poetry run python test.py, which should exhibit the bug and displays a column named 'val'

