from datetime import timedelta

from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.ml.feature import Bucketizer
from pyspark.sql import types
from pyspark.sql import Window

import model_utils as mu


DAYS_FROM_EULA_BINS = \
    [float('-Inf'), 6, 12, 21, 30, 45, 60, 80, 110, 140, 180, 250, 350, float('Inf')]
INT_TO_CHAR_BASELINE = 97

convert_to_char = F.udf(lambda x: chr(x), types.StringType())

TRIAL_SUCCESS_PAIR = types.StructType(
                        [types.StructField('trial', types.FloatType(), False),
                         types.StructField('success', types.FloatType(), False)])
pair_trial_success = F.udf(lambda t, s: (t, s), TRIAL_SUCCESS_PAIR)

calc_prob = F.udf(
    lambda trial, success, alpha, beta: (success + alpha) / (trial + alpha + beta),
    types.FloatType())


def get_table(sqlContext):
    return sqlContext.table('l2_sprint.mixpanel_home')


def load_received_notifications(events, start_date, end_date):
    # datetime objects are serializable only from spark 2.2.1
    content_items = events \
        .filter(events['event'] == 'ContentItem_Received'.lower()) \
        .filter(events['year'] >= start_date.year).filter(events['year'] <= end_date.year) \
        .filter(events['time'] >= start_date).filter(events['time'] <= end_date) \
        .filter(F.get_json_object(
            events['properties'], "$['properties']['IsTest']".lower()) == 'false') \
        .filter(events['MessageId'.lower()].isNotNull()) \
        .drop_duplicates(subset=['MessageId']) \
        .select(
            events['time'],
            events['CarrierName'.lower()].alias('carrier_name'),
            events['DaysFromEulaApproval'.lower()].astype('float').alias('days_from_eula'),
            events['MessageId'.lower()].alias('message_id'),
            events['MessageType'.lower()].alias('message_type'),
            events['DeviceVendor'.lower()].alias('device_vendor'),
        )

    print('received notification loaded')
    return content_items


def transform_data(content_items):
    content_items = content_items.withColumn('receive_date', F.to_date(F.col('time'))).drop('time')
    bucketizer = Bucketizer(splits=DAYS_FROM_EULA_BINS, inputCol='days_from_eula',
                            outputCol='days_from_eula_bin', handleInvalid='skip')
    content_items = bucketizer.transform(content_items) \
        .drop('days_from_eula') \
        .withColumn(
            'days_from_eula_bin',
            convert_to_char(F.col('days_from_eula_bin').astype('int') + INT_TO_CHAR_BASELINE)
        )

    print('content item data transformed')
    return content_items


def load_time_on_page(events, start_date, end_date):
    time_on_page = events \
        .filter(events['event'] == 'ContentItem_TimeOnPage'.lower()) \
        .filter(events['year'] >= start_date.year).filter(events['year'] <= end_date.year) \
        .filter(events['time'] >= start_date).filter(events['time'] <= end_date) \
        .filter(F.get_json_object(
            events['properties'], "$['properties']['IsTest']".lower()) == 'false') \
        .filter(events['MessageId'.lower()].isNotNull()) \
        .filter(F.get_json_object(
            events['properties'], "$['properties']['TimeOnPage']".lower()).isNotNull()) \
        .groupBy(F.col('MessageId'.lower()).alias('message_id')) \
        .agg(F.max(F.get_json_object(
            events['properties'], "$['properties']['TimeOnPage']".lower()).astype('float')
            ).alias('time_on_page'))

    print('TimeOnPage events loaded')
    return time_on_page


def calculate_success(data):
    return data.withColumn('success', F.when(F.col('time_on_page') >= 3, 1).otherwise(0)) \
        .drop('time_on_page')


def collect_data(events, start_date, end_date):
    content_items = load_received_notifications(events, start_date, end_date)
    content_items = transform_data(content_items)
    time_on_page = load_time_on_page(events, start_date, end_date + timedelta(days=2))
    success_action = calculate_success(time_on_page)

    content_items_full = content_items \
        .join(success_action, 'message_id', 'left') \
        .fillna(value=0, subset=['success']) \
        .fillna(value='unset') \
        .withColumn('trial', F.lit(1))

    print('received events join with TimeOnPage events done')
    return content_items_full


def groupby_hierarchy_sum(data, hierarchy):
    return data.groupBy(hierarchy) \
        .agg(F.sum('trial').alias('trial'), F.sum('success').alias('success'))


def groupby_hierarchy_sum_smoothed(data, hierarchy):
    return data.groupBy(hierarchy) \
        .agg(
            F.sum('trial').alias('trial'),
            F.sum('success').alias('success'),
            F.sum(F.col('trial_success_pair')['trial']).alias('trial_smooth'),
            F.sum(F.col('trial_success_pair')['success']).alias('success_smooth')) \
        .withColumn('trial_success_pair',
                    pair_trial_success(F.col('trial_smooth'), F.col('success_smooth'))) \
        .drop('trial_smooth', 'success_smooth')


def groupby_hierarchy_collect(data, hierarchy):
        return data.groupBy(hierarchy) \
            .agg(F.collect_list('trial_success_pair').alias('trial_success_list'))


def exponential_smoothing(data, gamma):
    end_date = data.agg(F.max('receive_date').alias('end_date')).collect()[0]['end_date']
    smoothing = F.udf(mu.exp_smoothing, types.FloatType())

    smoothed_data = data \
        .withColumn(
            'trial_success_pair',
            pair_trial_success(
                smoothing(F.col('trial'), F.lit(gamma), F.col('receive_date'), F.lit(end_date)),
                smoothing(F.col('success'), F.lit(gamma), F.col('receive_date'), F.lit(end_date))
            )
        )

    return smoothed_data


def filter_num_trials(data, hierarchy, threshold):
    if len(hierarchy) > 0:
        # TODO: check if better to filter on smoothed values (by average)
        filtered_trials = data.filter(F.col('trial') > threshold)

        diversity_window = Window.partitionBy(*hierarchy[:-1])
        # AnalysisException: Distinct window functions are not supported - size(collect_set) instead
        filtered = filtered_trials \
            .withColumn('num_types', F.size(F.collect_set(hierarchy[-1]).over(diversity_window))) \
            .filter(F.col('num_types') > 5) \
            .drop('num_types')
    else:
        filtered = data

    return filtered


def calc_priors(data_for_prior, sqlContext):
    extended_df = []
    for r in data_for_prior:
        r_dict = r.asDict()
        if len(data_for_prior[0]['trial_success_list']) == 1:
            alpha, beta = (0, 0)    # top of hierarchy, no prior
        else:
            alpha, beta = mu.calc_alpha_beta(r_dict['trial_success_list'], 0.5, 0.5)
        r_dict['alpha_beta'] = Row(**dict(zip(('alpha', 'beta'), (alpha, beta))))
        extended_df.append(Row(**r_dict))

    return sqlContext.createDataFrame(extended_df).drop('trial_success_list')


def get_model(data_for_prior, data_with_prior, hierarchy):
    if len(hierarchy) > 1:
        model = data_for_prior.join(data_with_prior, hierarchy[:-1])
    else:
        model = data_for_prior.crossJoin(data_with_prior)

    model = model \
        .withColumn('probability',
                    calc_prob(
                        F.col('trial_success_pair')['trial'],
                        F.col('trial_success_pair')['success'],
                        F.col('alpha_beta')['alpha'],
                        F.col('alpha_beta')['beta']))
    return model


def collect_model(model, hierarchy):
    return model.select(*hierarchy + ['probability']).collect()
