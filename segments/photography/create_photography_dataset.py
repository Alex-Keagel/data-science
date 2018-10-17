import os
import argparse

from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

from score_funcs import score_single_event


APPS_PATH = 's3://eds-atlas-telaviv-nonprod/DevSandbox/oleg/segments/photography/interestandroiddevices_flat/'


def get_dependencies():
    dirname = os.path.dirname(os.path.realpath(__file__))
    dependencies = []
    dependencies.append(os.path.join(dirname, 'score_funcs.py'))
    dependencies.append(os.path.join(dirname, 'ContentMetadataS3Retriever.py'))
    return dependencies


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_path',
                        help='output path in s3 including bucket without file name')
    args = parser.parse_args()
    if args.output_path:
        output_path = args.output_path
    else:
        raise ValueError('missing argument - output_path')

    model_name = 'photography'
    spark_config = SparkConf().setAppName(model_name)

    dependencies = get_dependencies()
    spark_context = SparkContext(conf=spark_config, pyFiles=dependencies)
    hive_context = HiveContext(spark_context)

    is_photo_related = F.udf(
        lambda s: True if ('camera' in s) or ('video' in s) else False, types.BooleanType())

    get_event_score = F.udf(score_single_event, types.FloatType())

    # received notification at least as many as viewed
    fix_received = F.udf(lambda received, view: max(received, view), types.FloatType())

    # TODO: switch to l1 home_events_uuid
    events = hive_context.table('l2_sprint.mixpanel_home')

    # choose photography related content interactions from notifications
    # (devicefeatures_attribute exists only for notification items, not future cards)
    # relevant content started approximately '2017-10-31'
    content_items = events \
        .filter(events['event'].isin(
            [x.lower() for x in ['ContentItem_Received', 'ContentItem_View', 'ContentItem_Click',
                                 'ContentItem_TimeOnPage', 'ContentItem_PageScroll']])
        ) \
        .filter(events['Time'.lower()] > '2017-10-31') \
        .filter(events['CarrierName'.lower()].isin('sprint', 'verizon')) \
        .filter(F.get_json_object(
            events['properties'], "$['properties']['IsTest']".lower()) == 'false') \
        .filter(F.get_json_object(
            events['properties'], "$['properties']['DeviceId']".lower()).isNotNull()) \
        .filter(F.get_json_object(
            events['properties'], "$['properties']['MessageType']".lower()).isNotNull()) \
        .filter(F.get_json_object(
            events['properties'], "$['properties']['devicefeatures_attribute']").isNotNull()) \
        .filter(is_photo_related(F.get_json_object(
            events['properties'], "$['properties']['devicefeatures_attribute']")))

    # assign score for each interactions
    content_items = content_items \
        .withColumn(
            'score',
            get_event_score(
                events['event'], F.get_json_object(events['properties'], "$['properties']"))
        )

    # aggregate score per user, item, event, action (action to differentiate clicks).
    # use max on properties for score because page scroll sends intermediate states for example.
    # use max on properties in case it's null or empty string in one of the events
    content_items = content_items \
        .groupBy(
            F.get_json_object(
                events['properties'], "$['properties']['DeviceId']".lower()).alias('device_id'),
            events['event'],
            F.get_json_object(
                events['properties'], "$['properties']['MessageType']".lower()).alias('topic'),
            F.get_json_object(
                events['properties'], "$['properties']['ActionId']".lower()).alias('action')
        ) \
        .agg(
            F.max(F.get_json_object(
                events['properties'], "$['properties']['AtlasUniqueUserId']".lower())).alias('user_id'),
            F.max('CarrierName'.lower()).alias('carrier_name'),
            F.max('DeviceModel'.lower()).alias('device_model'),
            F.max('DeviceModelName'.lower()).alias('device_model_name'),
            F.max('DeviceOsType'.lower()).alias('device_os_type'),
            F.max('DeviceVendor'.lower()).alias('device_vendor'),
            F.max('score').alias('score')
        )

    # FIXME fix view according action events
    received_content_items = content_items \
        .groupBy('device_id') \
        .pivot('event', ['ContentItem_Received'.lower(), 'ContentItem_View'.lower()]).sum('score') \
        .fillna(0.0) \
        .select(
            'device_id',
            fix_received(F.col('contentitem_received'), F.col('contentitem_view')).alias('receive'))

    # calculate final score for user.
    content_items = content_items \
        .filter(events['event'] != 'ContentItem_Received'.lower()) \
        .groupBy('device_id') \
        .agg(
            F.max('user_id').alias('user_id'),
            F.max('carrier_name').alias('carrier_name'),
            F.max('device_model').alias('device_model'),
            F.max('device_model_name').alias('device_model_name'),
            F.max('device_os_type').alias('device_os_type'),
            F.max('device_vendor').alias('device_vendor'),
            F.sum('score').alias('total_score')
        ) \
        .join(received_content_items, 'device_id', 'left') \
        .withColumn('score', F.round(F.col('total_score') / F.col('receive'))) \
        .drop('total_score', 'receive') \
        .withColumn('photography_interest', F.lit(None))

    # choose users who completed user interest questionnaire
    interests = events \
        .filter(events['event'] == 'Timeline_OnboardingMessage_Click'.lower()) \
        .filter(events['CarrierName'.lower()].isin('sprint', 'verizon')) \
        .filter(F.get_json_object(
            events['properties'], "$['properties']['IsTest']".lower()) == 'false') \
        .filter(F.get_json_object(
            events['properties'], "$['properties']['DeviceId']".lower()).isNotNull()) \
        .filter(F.get_json_object(
            events['properties'], "$['properties']['ActionId']".lower()) == 'done')

    # assign score for photography interest
    interests = interests \
        .withColumn(
            'score',
            get_event_score(
                events['event'], F.get_json_object(events['properties'], "$['properties']"))
        )

    # subset relevant properties and drop duplicated devices
    # (assuming each user should answer questionnaire ones)
    interests = interests \
        .select(
            F.get_json_object(
                events['properties'], "$['properties']['DeviceId']".lower()).alias('device_id'),
            F.get_json_object(
                events['properties'], "$['properties']['AtlasUniqueUserId']".lower()).alias('user_id'),
            events['CarrierName'.lower()].alias('carrier_name'),
            events['DeviceModel'.lower()].alias('device_model'),
            events['DeviceModelName'.lower()].alias('device_model_name'),
            events['DeviceOsType'.lower()].alias('device_os_type'),
            events['DeviceVendor'.lower()].alias('device_vendor'),
            'score'
        ) \
        .drop_duplicates(['device_id']) \
        .withColumn('photography_interest', F.when(F.col('score') > 0, 1.0).otherwise(0.0))

    # assregate content and interest scores
    # use max on properties in case it's null or empty string in one of the events
    photography_user = content_items.union(interests) \
        .groupBy('device_id') \
        .agg(
            F.max('user_id').alias('user_id'),
            F.max('carrier_name').alias('carrier_name'),
            F.max('device_model').alias('device_model'),
            F.max('device_model_name').alias('device_model_name'),
            F.max('device_os_type').alias('device_os_type'),
            F.max('device_vendor').alias('device_vendor'),
            F.sum('score').alias('score'),
            F.max('photography_interest').alias('photography_interest')
        )

    dgx = hive_context.table('l2_asurion.demographics_dbo_source_dgx')
    mobileid = hive_context.table('l3_sprint.mobileid')

    # FIXME: decrypt ethnicityrollup, dob, ethnicity
    photography_user_augmented = photography_user \
        .join(mobileid.select('mobileuid', 'subid'),
              photography_user['user_id'] == mobileid['mobileuid'],
              'left') \
        .join(dgx.select('source_dfx_id', 'nameprefix', 'state', 'age_range', 'income_range_vds',
                         'gender', 'marital_status', 'dwelling_type', 'home_ownership',
                         'length_of_residence', 'presence_of_children',
                         'mail_public_responder_indicator', 'mail_responsive_buyer_indicator',
                         'home_value_range', 'networthindicator_rollup', 'wealth_decile',
                         'homeandlandvalue', 'first_mortgage_amount', 'level_of_education',
                         'head_of_household', 'professionalrollup', 'premover',
                         'active_fitness_interest', 'golf_interest', 'traveler', 'green_advocate'),
              mobileid['subid'] == dgx['source_dfx_id'],
              'left')

    apps = hive_context.read.parquet(APPS_PATH)

    photography_user_augmented = photography_user_augmented \
        .join(apps, photography_user_augmented['device_id'] == apps['deviceId'], 'left')

    photography_user_augmented.write.csv('s3://' + output_path, mode='overwrite',
                                         compression='gzip', header=True)


if __name__ == '__main__':
    main()
