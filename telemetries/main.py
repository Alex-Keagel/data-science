import argparse
from pyspark import SparkContext, Row
from pyspark.sql import HiveContext


def load_telemetries_data(telemetries_path):
    return sql_context.read.json(telemetries_path + "/*").drop_duplicates(['deviceId'])


def get_parameters():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days_back', required=True, help='model days back for training & evaluation')
    parser.add_argument('--target_path', required=True, help='Target path to push files on s3')
    parser.add_argument('--external_lib', required=True, action='append', help='External Lib')
    parser.add_argument('--telemetries_s3_path', required=True, help='path to telemetries in s3')

    args = parser.parse_args()

    from_date = int(args.modeldaysBack)
    target_path = args.targetPath
    external_lib = args.externalLib
    telemetries_s3_path = args.telemetries_s3_path

    return from_date, \
           target_path, \
           external_lib, \
           telemetries_s3_path


if __name__ == "__main__":
    (from_date, target_path, external_lib, telemetries_path) = get_parameters()

    sc = SparkContext(appName="telemetries", pyFiles=external_lib)
    sql_context = HiveContext(sc)

    from telemetries.create_user_apps import explode_list_to_columns, filter_apps_by_threshold, pivot_df_by_dim_sum, \
        load_received_events, load_view_events, join_sent_with_view_events, join_events_to_apps

    telemtry_data = load_telemetries_data(telemetries_path)
    events = sql_context.table("l2_sprint.mixpanel_home")

    exploded_data = explode_list_to_columns(telemtry_data, 'categories')
    filtered_exploded_data = filter_apps_by_threshold(exploded_data, 200)
    apps_data_pivoted = pivot_df_by_dim_sum(filtered_exploded_data, 'name', 'value')

    received_events = load_received_events(events, from_date).drop_duplicates(subset=['MessageId'])
    view_events = load_view_events(events, from_date).drop_duplicates(subset=['ViewMessageId'])
    data = join_sent_with_view_events(received_events, view_events)
    data_joined = join_events_to_apps(data, apps_data_pivoted)

    data_joined.write.parquet(target_path, mode="overwrite")

    sc.stop()
