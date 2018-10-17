import argparse
from copy import deepcopy

from pyspark import SparkContext
from pyspark.sql import HiveContext

import spark_data_provider as sdp
import model_utils as mu
from AzureBlobUpdater import blobSharedForModel


MIN_TRIALS = 200


def get_parameters():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days_back', required=True, type=int,
                        help='model days back for training & evaluation')
    parser.add_argument('--end_date',
                        help='last date for model format YYYY-MM-DD (default: 4 days back)')
    parser.add_argument('--smoothing_factor', required=True, type=float, metavar='GAMMA',
                        help='smoothing factor for the exponential smoothing. 0 < gamma < 1')
    parser.add_argument('--hierarchy', nargs='+', required=True,
                        help='list of features in hierarchy ordered by level')
    parser.add_argument('--model_version', required=True, help='A model version string')

    return parser.parse_args()


def get_sqlContext():
    sc = SparkContext(appName='Hierarchical Model',
                      pyFiles=['/home/hadoop/HierarchicalModelMain/spark_data_provider.py',
                               '/home/hadoop/HierarchicalModelMain/model_utils.py'])
    return HiveContext(sc)


def main():
    args = get_parameters()
    sqlContext = get_sqlContext()
    events = sdp.get_table(sqlContext)

    start_date, end_date = mu.get_timewindow(args.days_back, args.end_date)

    prediction_tree = {}
    prediction_tree['days_back'] = args.days_back
    prediction_tree['end_date'] = args.end_date
    prediction_tree['smoothing_factor'] = args.smoothing_factor
    prediction_tree['hierarchy'] = deepcopy(args.hierarchy)

    data = sdp.collect_data(events, start_date, end_date)
    data.cache()

    hierarchy = args.hierarchy

    # exponential smoothing
    hierarchy.append('receive_date')
    grouped = sdp.groupby_hierarchy_sum(data, hierarchy)
    grouped = sdp.exponential_smoothing(grouped, args.smoothing_factor)
    hierarchy.pop()
    print('exponential smoothing done')

    while(True):
        print('working on {}'.format(hierarchy))
        grouped = sdp.groupby_hierarchy_sum_smoothed(grouped, hierarchy)
        grouped.cache()
        filtered_data_for_prior = sdp.filter_num_trials(grouped, hierarchy, MIN_TRIALS)
        data_for_prior_local = \
            sdp.groupby_hierarchy_collect(filtered_data_for_prior, hierarchy[:-1]).collect()
        if len(data_for_prior_local) > 0:
            data_with_prior = sdp.calc_priors(data_for_prior_local, sqlContext)
            model = sdp.get_model(grouped, data_with_prior, hierarchy)
            model_local = sdp.collect_model(model, hierarchy)
            for r in model_local:
                mu.update_dict_rec(hierarchy, r, prediction_tree)
        try:
            hierarchy.pop()
        except Exception:
            break   # hierarchy is empty. model is done

    blobSharedForModel(prediction_tree, 'Hierarchical', args.model_version)


if __name__ == '__main__':
    main()
