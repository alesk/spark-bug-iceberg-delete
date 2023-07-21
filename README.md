# Spark Bug - Iceberg Table `MERGE INTO` Statement with Null Values

This repository demonstrates a bug in Spark when using the `MERGE INTO ... WHEN MATCHED THEN DELETE` statement on an
Iceberg table. The bug occurs when deleting rows from the Iceberg table, and one of the remaining rows contains a null
value in some column. The issue is reproducible only when the `spark.default.parallelism` configuration is set to 1.

## Bug Details

- Spark Version: 3.4.1
- Iceberg Version: 1.3
- Reproducible with `spark.default.parallelism=1`
- Works fine with Iceberg 1.3 and Spark 3.3.2
- Bug is caused by the `MERGE INTO ... WHEN MATCHED THEN DELETE` statement deleting rows from the Iceberg table.
- Bug is manifested as an exception thrown by the Spark job

```text
java.lang.NullPointerException: Cannot invoke "org.apache.spark.unsafe.types.UTF8String.getBaseObject()" because "input" is null
```

## Bug Reproduction

1. Make sure you have python 3.7+ installed.
2. Run the provided script `run.sh` to run the Spark job with the bug.

## Bug Workaround

There is a workaround to this bug. Instead of using the `MERGE INTO ... WHEN MATCHED THEN DELETE` statement, you can use
the `DELETE FROM ... WHERE EXISTS` statement to achieve the same functionality without encountering the bug.

## Running on a Different Spark/Iceberg Version

If you want to run the code on different spark configuration, you can use the following command:

In `run.sh` change the line 7 to install the different version of pyspark, i.e. `pyspark==3.3.2`

In min_reproduce, change the line 18 installing different version of iceberg runtime, for example

```
'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0'
```

## Repository Contents

The repository contains the following files:

1. `min_reproduce.py`: Python script to reproduce the bug.
2. `run.sh`: Bash script to execute the bug reproduction.
4. `README.md`: This README file.

## Actions taken

An [issue #8126](https://github.com/apache/iceberg/issues/8126) has been submitted to the Apache Iceberg's GitHub
repository.