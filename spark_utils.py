from pyspark.sql import SparkSession


def get_data_source_path(resources_folder, run_date, data_source_type):
    return "{}/{}/day={}".format(resources_folder, data_source_type, run_date.day)


def ingest_data(spark, *data_source_path):
    return spark.read.csv(*data_source_path, header=True)


def get_or_create_spark_session():
    return (SparkSession
            .builder
            .appName("app")
            .config("spark.default.parallelism", 4)
            .config("spark.sql.shuffle.partitions", 4)
            .config("spark.cores.max", "2")
            .config("spark.executor.instances", "2")
            .config("spark.executor.memory", "2g")
            .config("spark.executor.cores", "2")

            .getOrCreate())
