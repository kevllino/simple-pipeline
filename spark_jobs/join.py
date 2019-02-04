import argparse
from datetime import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from spark_utils import *


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

    spark = get_or_create_spark_session()
    add_df = ingest_data(spark, get_data_source_path('gs://' + known_args.bucket + '/data',run_date, data_source_type="result"))
    sub_df = ingest_data(spark, get_data_source_path('gs://' + known_args.bucket + '/data',run_date, data_source_type="result_sub"))
    df = add_df.join(sub_df, 'key', 'inner')

    df.write.mode('overwrite').csv(
        'gs://' + known_args.bucket + "/data/finale/day={}".format(run_date.day), header=True)


