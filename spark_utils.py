import os
from pyspark.sql import SparkSession

# from main_dag import BUCKET

ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# RESOURCES_FOLDER = 'gs://' + BUCKET + "/data"


def get_data_source_path(run_date, data_source_type):
    return "data/{}/day={}".format(data_source_type, run_date.day)


def ingest_data(spark, *data_source_path):
    return spark.read.csv(*data_source_path)
