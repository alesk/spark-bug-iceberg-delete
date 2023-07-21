import pyspark
import tempfile

def get_spark_session(derby_dir, warehouse_dir):
    """
    Get or create a Spark session with the necessary configurations for reproducing the bug.

    Parameters:
        derby_dir (str): Directory path for the temporary Derby database.
        warehouse_dir (str): Directory path for the temporary Iceberg warehouse.

    Returns:
        pyspark.sql.SparkSession: The Spark session.
    """
    conf_dict = {
        # Spark and Iceberg configurations
        'spark.app.name': 'spark_bug_001',
        'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0',
        'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.iceberg.type': 'hadoop',
        'spark.sql.catalog.iceberg.warehouse': f'{warehouse_dir}',
        'spark.driver.extraJavaOptions': f'-Dderby.system.home={derby_dir}',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.driver.host': 'localhost',
        'spark.driver.memory': '512m',
        'spark.sql.shuffle.partitions': '1',
        'spark.default.parallelism': '1',  # This configuration causes the bug
        'spark.rdd.compress': False,
        'spark.shuffle.compress': False,
        'spark.ui.enabled': False,
        'spark.ui.showConsoleProgress': False
    }

    conf = pyspark.SparkConf().setAll(list(conf_dict.items()))
    return pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()

def reproduce_bug(spark):
    """
    Reproduce the bug in Spark when using the `MERGE INTO ... WHEN MATCHED THEN DELETE` statement.

    Parameters:
        spark (pyspark.sql.SparkSession): The Spark session.
    """
    # Create a Spark DataFrame with a nullable column
    dst_df = spark.createDataFrame([(1, 'm'), (3, None), (1, None)], ['id', 'nullable_string'])

    # Create another DataFrame with ids to be deleted
    remove_list_df = spark.createDataFrame([(1, )], ['id'])

    remove_list_df.createOrReplaceTempView('remove_list')
    dst_df.writeTo('iceberg.default.dst').createOrReplace()

    # Delete all rows where id is included in remove_list_df
    # If one of the rows has a column with null, an exception is thrown
    #
    #   java.lang.NullPointerException: Cannot invoke "org.apache.spark.unsafe.types.UTF8String.getBaseObject()" because "input" is null
    #
    # This bug is not reproducible if spark.default.parallelism is set to 2 or higher

    delete_sql = """
    MERGE INTO iceberg.default.dst AS a
    USING remove_list AS b
    ON
        a.id = b.id
    WHEN MATCHED THEN DELETE
        """

    # This works even with spark.default.parallelism = 1

    # delete_sql = """
    #    DELETE FROM iceberg.default.dst WHERE EXISTS (select 1 from remove_list where remove_list.a = dst.a)
    #    """

    spark.sql(delete_sql).show()

def main():
    with tempfile.TemporaryDirectory() as derby_dir:
        with tempfile.TemporaryDirectory() as warehouse_dir:
            spark = get_spark_session(derby_dir, warehouse_dir)
            reproduce_bug(spark)

if __name__ == '__main__':
    main()