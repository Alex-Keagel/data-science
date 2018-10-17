from datetime import timedelta
from math import log

import pyspark.sql.functions as F
from pyspark.sql import types

import spark_data_provider as sdp


def load_evaluation(events, start_date, end_date):
    content_items = events \
        .filter(events['event'] == 'ContentItem_Received'.lower()) \
        .filter(events['year'] >= start_date.year).filter(events['year'] <= end_date.year) \
        .filter(events['time'] >= start_date).filter(events['time'] <= end_date) \
        .filter(F.get_json_object(
            events['properties'], "$['properties']['IsTest']".lower()) == 'false') \
        .filter(events['MessageId'.lower()].isNotNull()) \
        .filter(F.get_json_object(
            events['properties'], "$['properties']['ModelId']".lower()) == 'WaterfallTopRank'.lower()) \
        .drop_duplicates(subset=['MessageId']) \
        .select(
            events['time'],
            events['CarrierName'.lower()].alias('carrier_name'),
            events['DaysFromEulaApproval'.lower()].astype('float').alias('days_from_eula'),
            events['ProgramName'.lower()].alias('program_name'),
            events['MessageId'.lower()].alias('message_id'),
            events['MessageType'.lower()].alias('message_type'),
            events['DeviceVendor'.lower()].alias('device_vendor'),
            F.get_json_object(
                events['properties'], "$['properties']['InstallationSource']".lower()
            ).alias('installation_source'),
            F.get_json_object(
                events['properties'], "$['properties']['ModelRecommendationProbability']".lower()
            ).cast('float').alias('waterfall_p')
        ) \
        .fillna(value='unset')      # FIXME: empty string value

    print('received notification loaded')
    return content_items


def log_loss(p, y):
    p = max(min(p, 1. - 1e-10), 1e-10)
    return -log(p) if y == 1. else -log(1. - p)


def add_metrics(eval_data):
    log_loss_udf = F.udf(log_loss, types.FloatType())
    return eval_data \
        .withColumn('log_loss', log_loss_udf(F.col('waterfall_p'), F.col('success'))) \
        .withColumn('mse', F.pow(F.col('success') - F.col('waterfall_p'), 2))


def collect_data(events, start_date, end_date):
    # FIXME: ~81.5% of TimeOnPage arrive on the same day, ~10.8% after 1 day, ~6% after 2<= <= 7 days
    content_items = load_evaluation(events, start_date, end_date)
    content_items = sdp.transform_data(content_items)
    time_on_page = sdp.load_time_on_page(events, start_date, end_date + timedelta(days=2))
    success_action = sdp.calculate_success(time_on_page)

    content_items_full = content_items \
        .join(success_action, 'message_id', 'left') \
        .fillna(0, subset=['success'])

    content_items_full = add_metrics(content_items_full)

    print('received events join with TimeOnPage events done')
    return content_items_full


def get_baseline_metrics(eval_data):
    metrics = eval_data \
        .agg(F.mean('log_loss').alias('baseline_log_loss'),
             F.mean('mse').alias('baseline_mse')) \
        .collect()
    return metrics[0].asDict()


def get_proba(order, probability, fallback_probability):
    if len(order) > 0 and order[0] is not None and order[0] in probability:
        fallback_p = probability.get('probability', fallback_probability)
        return get_proba(order[1:], probability[order[0]], fallback_p)
    return probability.get('probability', fallback_probability)


def get_model_metrics(eval_data, prediction_tree):
    collect_data = eval_data.select(prediction_tree['hierarchy'] + ['success']).collect()

    total_log_loss = 0
    total_mse = 0
    for r in collect_data:
        try:
            features = [r[i] for i in prediction_tree['hierarchy']]
            p = get_proba(features, prediction_tree, prediction_tree['probability'])
            total_log_loss += log_loss(p, r['success'])
            total_mse += (r['success'] - p) ** 2
        except Exception as e:
            print(r)

    return {'model_log_loss': total_log_loss / len(collect_data),
            'model_mse': total_mse / len(collect_data)}
