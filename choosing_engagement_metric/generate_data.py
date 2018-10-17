import argparse
import re
import boto3
from pyspark import SparkContext, HiveContext
import json
from utils.soluto_aws_utils.soluto_aws_utils.athena import run_query

DATABASE = 'tlv_engagement_data'


def strip_margin(text):
    return re.sub('\n[ \t]*\|', '\n', text)


def fill_date_placeholders(string, placeholder_vlaue):
    return string.format(date_placeholder=placeholder_vlaue)


def push_data_to_s3(min_date, query_placeholder, s3_output):
    athena_client = boto3.client('athena')
    query = fill_date_placeholders(strip_margin(query_placeholder),min_date)
    return run_query(athena_client, query, DATABASE, s3_output)


def get_parameters():
    parser = argparse.ArgumentParser()
    parser.add_argument('--from_date', required=False, default=None ,help='min date ')
    parser.add_argument('--target_path', required=True, help='Target path to push files on s3')
    parser.add_argument('--external_lib', required=True, action='append', help='External Lib')

    args = parser.parse_args()

    from_date = args.from_date
    target_path = args.target_path
    external_lib = args.external_lib

    return from_date, target_path, external_lib

if __name__ == "__main__":
    (from_date, target_path, external_lib) = get_parameters()

    sc = SparkContext(appName="Engagement metric - generate data", pyFiles=external_lib)
    sqlContext = HiveContext(sc)
    from utils.soluto_log.soluto_log import get_logger
    from choosing_engagement_metric.resources.raw_data_query import query_with_percentiles

    response = push_data_to_s3(from_date, query_with_percentiles, target_path)
    logger = get_logger()
    logger.info('Response: {}'.format((json.dumps(response, indent=4, sort_keys= True))))
    sc.stop()
