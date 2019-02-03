def get_data_source_path(resources_folder, run_date, data_source_type):
    return "{}/{}/day={}".format(resources_folder, data_source_type, run_date.day)


def ingest_data(spark, *data_source_path):
    return spark.read.csv(*data_source_path)
