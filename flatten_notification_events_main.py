import argparse

from pyspark import SparkContext
from pyspark.sql import HiveContext

def get_date_n_days_ago(daysAgo):
    date_N_days_ago = datetime.now() - timedelta(days=daysAgo)
    return date_N_days_ago.date().strftime('%Y-%m-%d')

def get_latest_partition_date(path):
    latest_partition = get_latest_partition(DEFAULT_BUCKET,path)
    date = date_parser.parse(latest_partition.split('/date=')[1].split('/')[0]) + timedelta(days=1)
    return date.strftime('%Y-%m-%d')



def get_parameters():
    parser = argparse.ArgumentParser()
    parser.add_argument('--from_date', required=False, default=None ,help='min date ')
    parser.add_argument('--to_date', required=False, default=None ,help='max date')
    parser.add_argument('--target_path', required=True, help='Target path to push files on s3')
    parser.add_argument('--external_lib', required=True, action='append', help='External Lib')
    parser.add_argument('--write_mode', required=False, default='append', help=' df.write mode ->  values: append / overwrite when default=append')

    args = parser.parse_args()

    from_date = args.from_date
    to_date = args.to_date
    target_path = args.target_path
    external_lib = args.external_lib
    write_mode = args.write_mode

    return from_date, to_date, target_path, external_lib, write_mode



if __name__ == "__main__":
    (from_date, to_date, target_path, external_lib, write_mode) = get_parameters()

    sc = SparkContext(appName="flat_mixpanel_home_events", pyFiles=external_lib)
    sqlContext = HiveContext(sc)
    from flaten_mixpanel_home_events.s3_utils import DATA_PATH, DEFAULT_BUCKET, get_latest_partition
    from datetime import datetime, timedelta
    from dateutil import parser as date_parser

    if from_date is None:
        from_date = get_latest_partition_date(DATA_PATH)
    if to_date is None:
        to_date = get_date_n_days_ago(1)
    if to_date==from_date:
        to_date = (date_parser.parse(to_date)+ timedelta(days=1)).strftime('%Y-%m-%d')

    print('from_date = {} | to_date = {}'.format(from_date, to_date))

    from flaten_mixpanel_home_events.data_provider import load_sent_events, load_received_events, load_dismiss_events, load_view_events, \
        load_time_on_page_events, load_page_scroll_events, load_feedbackmodule_impression_events, \
        load_feedbackmodule_click_events, load_click_events, join_all_events
    from flaten_mixpanel_home_events.events_schema import sent_record_example, flatten_data_record_example
    from flaten_mixpanel_home_events.pyspark_schema_utils import rdd_to_df

    events = sqlContext.table("l2_sprint.mixpanel_home")
    sent_events = load_sent_events(events, from_date, to_date, sent_record_example, sqlContext)\
        .drop_duplicates(subset=['message_id'])
    received_events = load_received_events(events, from_date, to_date)\
        .drop_duplicates(subset=['received_message_id'])
    dismiss_events = load_dismiss_events(events, from_date, to_date)\
        .drop_duplicates(subset=['dismiss_message_id'])
    view_events = load_view_events(events, from_date, to_date)\
        .drop_duplicates(subset=['view_message_id'])
    time_on_page_events = load_time_on_page_events(events, from_date, to_date)\
        .drop_duplicates(subset=['time_on_page_message_id'])
    page_scroll_events = load_page_scroll_events(events, from_date, to_date)\
        .drop_duplicates(subset=['page_scroll_message_id'])
    feedbackmodule_impression_events = load_feedbackmodule_impression_events(events, from_date, to_date)\
        .drop_duplicates(subset=['feedback_module_impression_message_id'])
    feedbackmodule_click_events = load_feedbackmodule_click_events(events, from_date, to_date)\
        .drop_duplicates(subset=['feedback_module_click_message_id'])
    click_events = load_click_events(events, from_date, to_date)\
        .drop_duplicates(subset=['click_message_id'])
    flatten_data_rdd = join_all_events(sent_events,
                                   received_events,
                                   dismiss_events,
                                   view_events,
                                   time_on_page_events,
                                   page_scroll_events,
                                   feedbackmodule_impression_events,
                                   feedbackmodule_click_events,
                                   click_events).rdd.map(lambda x: x.asDict())
    df = rdd_to_df(flatten_data_rdd, flatten_data_record_example, sqlContext)
    print('writing data to: {}'.format(target_path))
    df.write.partitionBy("date").parquet(target_path, mode=write_mode)
    print('data was uploaded, writing mode = {}'.format(write_mode))
    sc.stop()
