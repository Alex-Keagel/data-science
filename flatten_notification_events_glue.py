from pyspark.sql import HiveContext

from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from flaten_mixpanel_home_events.data_provider import load_sent_events, load_received_events, load_dismiss_events, load_view_events, \
    load_time_on_page_events, load_page_scroll_events, load_feedbackmodule_impression_events, \
    load_feedbackmodule_click_events, load_click_events, join_all_events
from telemetries.events_schema import sent_record_example, flatten_data_record_example
from flaten_mixpanel_home_events.pyspark_schema_utils import rdd_to_df



sc = SparkContext(appName="telemetries", pyFiles=external_lib)
sqlContext = HiveContext(sc)
glueContext = GlueContext(sc)


events = sqlContext.table("l2_sprint.mixpanel_home")
sent_events = load_sent_events(events, from_date, to_date, sent_record_example, sqlContext)
received_events = load_received_events(events, from_date, to_date)
dismiss_events = load_dismiss_events(events, from_date, to_date)
view_events = load_view_events(events, from_date, to_date)
time_on_page_events = load_time_on_page_events(events, from_date, to_date)
page_scroll_events = load_page_scroll_events(events, from_date, to_date)
feedbackmodule_impression_events = load_feedbackmodule_impression_events(events, from_date, to_date)
feedbackmodule_click_events = load_feedbackmodule_click_events(events, from_date, to_date)
click_events = load_click_events(events, from_date, to_date)
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
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "new_dynamic_frame")
glueContext.write_dynamic_frame.from_options(
       frame = dynamic_frame,
       connection_type = "s3",
       connection_options = {"path": "s3://eds-atlas-telaviv-nonprod/DevSandbox/flatten_events_partitioned_by_date_glue_test"},
       format = "parquet")
df.write.partitionBy("date").parquet("s3://eds-atlas-telaviv-nonprod/DevSandbox/flatten_events_partitioned_by_date_test", mode="overwrite")
df.write.partitionBy("date").parquet("s3://eds-atlas-telaviv-nonprod/DevSandbox/flatten_events_partitioned_by_date_test", mode="append")
df.write.parquet(target_path, mode="overwrite")

sc.stop()
