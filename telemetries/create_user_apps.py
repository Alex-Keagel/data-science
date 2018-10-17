import json
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window



# Generate list of values from list of jsons by specific key
from pyspark_utils.date_time_utils import timestamp_str_to_date_str


def get_values_from_list(json_array, key):
    list = []
    try:
        for i in range(len(json_array)):
            list.append((json_array[i][key].lower().replace("&amp;", "and")
                         .replace(" ", "_")
                         .replace("_y_", "_and_")
                         .replace("_e_", "_and_"),
                         1))
    finally:
        return list


# Load received events
def load_received_events(events, fromDate):
    received_events = events.filter(events.event == "contentitem_received") \
        .filter(events.time >= fromDate) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('deviceostype', 'nonandroid') == 'android') \
        .filter(lambda x: x.get('daysfromeulaapproval', None) != None) \
        .map(lambda x: Row(CarrierName=x['carriername'],
                           DeviceId=x['deviceid'],
                           DaysFromEulaApproval=x.get('daysfromeulaapproval', 0),
                           ProgramName=x.get('programname', 'unset'),
                           MessageId=x['messageid'],
                           MessageType=x["messagetype"],
                           DeviceVendor=x['devicevendor'])) \
        .toDF().dropna()
    print('Received events loaded')
    return received_events

def load_sent_events(events, fromDate, toDate):
    sent_events = events.filter(events.event == "contentitem_sent") \
        .filter((events.time >= fromDate) & (events.time <= toDate)) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('deviceostype', 'nonandroid') == 'android') \
        .filter(lambda x: x.get('daysfromeulaapproval', None) != None) \
        .map(lambda x: Row(deviceOsVersion=x['deviceosversion'],
                           deviceId=x['deviceid'],
                           daysFromEulaApproval=x.get('daysfromeulaapproval', 0),
                           programName=x.get('programname', 'unset'),
                           date=timestamp_str_to_date_str(x.get('timestamp', '')),
                           registrationRegion=x['registrationregion'],
                           registrationCity=x["registrationcity"],
                           installationSource=x["installationsource"],
                           registrationCountry=x['registrationcountry'],
                           notificationCopyAttributes=x.get('notificationcopyattributes', None))) \
        .toDF()
    print('sent events loaded')
    return sent_events


def loadViewEvents(events, fromDate):
    viewEvents = events.filter(events.event == "contentitem_view") \
        .filter(events.time >= fromDate) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('daysfromeulaapproval', None) != None) \
        .filter(lambda x: x.get('messageid', None) is not None) \
        .map(lambda x: Row(ViewMessageId=x['messageid'])) \
        .toDF()
    print('View events loaded')
    return viewEvents


def loadTimeOnPageEvents(events, fromDate):
    viewEvents = events.filter(events.event == "contentitem_timeonpage") \
        .filter(events.time >= fromDate) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('daysfromeulaapproval', None) != None) \
        .filter(lambda x: x.get('messageid', None) is not None) \
        .map(lambda x: Row(TimeOnPageMessageId=x['messageid'],
                           TimeOnPage=x.get('timeonpage', 0))) \
        .toDF()
    print('TimeOnPage events loaded')
    return viewEvents


def loadLatestDeviceData(data):
    return data.rdd.map(lambda x: (x.deviceId, x)) \
        .reduceByKey(lambda x1, x2: max(x1, x2, key=lambda x: x.daysFromEulaApproval)) \
        .map(lambda x: x[1]).toDF()


def create_date_users_map(events):
    window = Window\
        .partitionBy(events.deviceId)\
        .orderBy(events.date.desc()).rowsBetween(-1,0)
    date_users_data = events.withColumn('previousDate', F.max(events.date).over(window))
    return date_users_data


# load content item view events
def load_view_events(events, fromDate):
    view_events = events.filter(events.event == "contentitem_view") \
        .filter(events.time >= fromDate) \
        .rdd.map(lambda x: json.loads(x.properties)['properties']) \
        .filter(lambda x: x['istest'] is False) \
        .filter(lambda x: x.get('daysfromeulaapproval', None) != None) \
        .filter(lambda x: x.get('messageid', None) is not None) \
        .map(lambda x: Row(ViewMessageId=x['messageid'])) \
        .toDF()
    print('View events loaded')
    return view_events


# left join between recieved & view
def join_sent_with_view_events(received, view):
    received_view_joined = received.join(view, received.MessageId == view.ViewMessageId, 'left')
    received_view = received_view_joined.withColumn('PreviewClicked',
                                            F.when(received_view_joined.ViewMessageId != "None", 1).otherwise(0)) \
        .drop('ViewMessageId')
    print('Sent events join with view events done')
    return received_view


# inner join telemetries data to events data by userId
def join_events_to_apps(events, apps):
    joined = events.join(apps, events.DeviceId == apps.deviceId, 'inner').drop('deviceId')
    return joined


# Transform telemetries data for structued data frame, take the column (dim) that contains a list and explode it
# into multiple rows -> name - categorical column of the dim, value = 1 for all
def explode_list_to_columns(df, dim):
    data = df.rdd.map(lambda row: Row(deviceId=row.deviceId,
                                      packages=row.packages,
                                      apps=get_values_from_list(row.packages, 'package'),
                                      categories=get_values_from_list(row.packages, 'category'))) \
        .toDF()

    exploded = data.withColumn('exploded', F.explode(dim))
    exploded = exploded \
        .withColumn('name', F.col('exploded')._1) \
        .withColumn('value', F.col('exploded')._2)

    if dim == 'apps':
        # .vpl = Virtual Preload -> not really apps
        is_vpl = F.udf(lambda x: False if x.endswith('.vpl') else True, BooleanType())
        exploded = exploded.filter(is_vpl(exploded['name']))

    return exploded


# Filter rows by column and min threshold
def filter_data_frame_by_numeric_value(data, numeric_column, min_val):
    return data.where(data[numeric_column] > min_val)


# Filter app/categories rows that we do not have enough observations on them
def filter_apps_by_threshold(df, threshold):
    grouped = df.groupBy('name').agg(F.count('name')).withColumnRenamed("name", "name_grouped") \
        .withColumnRenamed("count(name)", "count")
    joined = df.join(grouped, df.name == grouped.name_grouped, 'left').drop('name_grouped')
    df_filtered = filter_data_frame_by_numeric_value(joined, 'count', threshold).drop('count')
    return df_filtered


# Pivot the data frame -> create columns from one column values. the values are the sum of appearances of the column
# in the un-pivoted data frame
def pivot_df_by_dim_sum(df, pivot_column_name, values_column_to_agg):
    return df.groupby('deviceId').pivot(pivot_column_name).agg(F.sum(values_column_to_agg)).na.fill(0)


# Pivot the data frame -> create columns from one column values. values are 1 - if at least one appearance of the column
# appeared in the un-pivoted data frame, else - 0
def pivot_df_by_dim_binary(df, pivot_column_name, values_column_to_agg):
    return df.groupby('deviceId').pivot(pivot_column_name).agg(F.max(values_column_to_agg)).na.fill(0)
