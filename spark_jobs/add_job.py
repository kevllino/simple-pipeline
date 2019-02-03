import argparse
from datetime import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from spark_utils import ingest_data, get_data_source_path


def add_to_df(df):
    return (df
     .withColumnRenamed("_c0", "key")
     .withColumnRenamed("_c1", "value")
     .withColumn("value", f.col("value") + f.lit(100))
     )



if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--run_date', dest='run_date',
                        required=True,
                        help='run_date')

    parser.add_argument('--bucket', dest='bucket',
                        required=True,
                        help='bucket')

    known_args, _ = parser.parse_known_args(None)
    run_date = datetime.strptime(known_args.run_date[:10], '%Y-%m-%d')
    print("run_date is " + str(run_date))

    spark = SparkSession.builder.appName("ingestion").getOrCreate()
    df = ingest_data(spark, get_data_source_path('gs://' + known_args.bucket + '/data',run_date, data_source_type="raw"))
    enhanced_df = add_to_df(df)
    enhanced_df.write.mode('overwrite').csv('gs://' + known_args.bucket + "/data/result/day={}".format(run_date.day))

