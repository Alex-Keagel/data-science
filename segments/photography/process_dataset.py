import argparse
import logging

import pandas as pd
import numpy as np
from sklearn.metrics import average_precision_score

import utils
import models
import soluto_log


def parse_args():
    parser = argparse.ArgumentParser(description='predictive model testing for photography segment')
    parser.add_argument('data_path', help='path to directory with the raw data')

    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('--folds',  required=True, type=int,
                               help='number of folds in K-fold cross-validation')
    requiredNamed.add_argument('--test_size', type=float,
                               help='portion of test set out of all records')

    return parser.parse_args()


def models_variations(X_train, X_test, y_train, y_test, folds):
    logger = logging.getLogger('log')

    # NOTE: X sets might contain raws with only zeros.

    apps = utils.get_apps(X_train.columns)
    google_apps = [i for i in apps if 'google.android' in i]
    logger.debug('apps with "google.android" in package name:\n{}'.format(google_apps))
    carrier_apps_pattern = set([i for i in X_train['carrier_name'].unique()])
    carrier_apps = [i for i in apps if (carrier_apps_pattern & set(i.split('.')))]
    logger.debug('apps with a carrier name in package name:\n{}'.format(carrier_apps))
    # .sec. is for samsung apps.
    vendor_apps_pattern = set([i for i in X_train['device_vendor'].unique()] + ['sec'])
    vendor_apps = [i for i in apps if (vendor_apps_pattern & set(i.split('.')))]
    logger.debug('apps with a vendor name in package name:\n{}'.format(vendor_apps))

    pre_installed = {}
    for property in ['carrier_name', 'device_model', 'device_vendor']:
        pre_installed_apps = \
            utils.get_top_by_property(X_train[[property] + apps], property, 100, 0.9)
        pre_installed[property] = pre_installed_apps
        logger.debug('{property} pre installed apps:\n{apps}'.format(
            property=property, apps=pre_installed_apps.to_string()))

    # hot-one encode device model
    # NOTE: X_train and X_test might have inconsistent columns due to rare device models
    device_models = X_train['device_model']
    X_train = pd.get_dummies(X_train, columns=['device_model'], prefix='', prefix_sep='')
    X_train = pd.concat([X_train, device_models], axis=1)
    logger.info('train shape: {}'.format(X_train.shape))
    device_models_test = X_test['device_model']
    X_test = pd.get_dummies(X_test, columns=['device_model'], prefix='', prefix_sep='')
    X_test = pd.concat([X_test, device_models_test], axis=1)
    device_models = X_train['device_model'].unique()

    logger.info('all apps ; devices models with variance > 0.0099')
    filtered_models = utils.filter_low_var_columns(X_train[device_models], 0.0099)
    features = np.append(filtered_models, apps)
    model_df = X_train[features].copy()
    model = models.logistic_regression(model_df, y_train, folds)
    logger.info('test average precision: {}'.format(
        average_precision_score(y_test,
                                model.predict_proba(X_test[model_df.columns])[:, 1])))

    logger.info('apps with variance > 0.09 ; devices models with variance > 0.0099')
    filtered_apps = utils.filter_low_var_columns(X_train[apps], 0.09)
    filtered_models = utils.filter_low_var_columns(X_train[device_models], 0.0099)
    features = np.append(filtered_models, filtered_apps)
    model_df = X_train[features].copy()
    model = models.logistic_regression(model_df, y_train, folds)
    logger.info('test average precision: {}'.format(
        average_precision_score(y_test,
                                model.predict_proba(X_test[model_df.columns])[:, 1])))

    logger.info('apps with variance > 0.09 ; devices models with variance > 0.0099 ; clean rows with 0 apps')
    filtered_apps = utils.filter_low_var_columns(X_train[apps], 0.09)
    filtered_models = utils.filter_low_var_columns(X_train[device_models], 0.0099)
    features = np.append(filtered_models, filtered_apps)
    model_df = X_train[features].copy()
    model_df = model_df[model_df.sum(axis=1) > 0]
    model = models.logistic_regression(model_df, y_train, folds)
    logger.info('test average precision: {}'.format(
        average_precision_score(y_test,
                                model.predict_proba(X_test[model_df.columns])[:, 1])))

    logger.info('apps with variance > 0.09 + remove apps by name ; devices models with variance > 0.0099')
    filtered_apps = [i for i in apps if i not in google_apps + carrier_apps + vendor_apps]
    filtered_apps = utils.filter_low_var_columns(X_train[filtered_apps], 0.09)
    filtered_models = utils.filter_low_var_columns(X_train[device_models], 0.0099)
    features = np.append(filtered_models, filtered_apps)
    model_df = X_train[features].copy()
    model = models.logistic_regression(model_df, y_train, folds)
    logger.info('test average precision: {}'.format(
        average_precision_score(y_test,
                                model.predict_proba(X_test[model_df.columns])[:, 1])))

    logger.info('apps with variance > 0.09 + remove apps by name ; devices models with variance > 0.0099 ; clean rows with 0 apps')
    filtered_apps = [i for i in apps if i not in google_apps + carrier_apps + vendor_apps]
    filtered_apps = utils.filter_low_var_columns(X_train[filtered_apps], 0.09)
    filtered_models = utils.filter_low_var_columns(X_train[device_models], 0.0099)
    features = np.append(filtered_models, filtered_apps)
    model_df = X_train[features].copy()
    model_df = model_df[model_df.sum(axis=1) > 0]
    model = models.logistic_regression(model_df, y_train, folds)
    logger.info('test average precision: {}'.format(
        average_precision_score(y_test,
                                model.predict_proba(X_test[model_df.columns])[:, 1])))

    logger.info('remove apps by pre installed ; devices models with variance > 0.0099')
    model_df = X_train.copy()
    model_df_test = X_test.copy()
    for property in ['carrier_name', 'device_model', 'device_vendor']:
        for p, apps_replace in pre_installed[property].groupby(level=0):
            apps_replace_list = apps_replace.index.get_level_values('level_1').tolist()
            model_df.loc[model_df[property] == p, apps_replace_list] = 0
            model_df_test.loc[model_df_test[property] == p, apps_replace_list] = 0
    filtered_models = utils.filter_low_var_columns(model_df[device_models], 0.0099)
    features = np.append(filtered_models, apps)
    model_df = model_df[features]
    model = models.logistic_regression(model_df, y_train, folds)
    logger.info('test average precision: {}'.format(
        average_precision_score(y_test,
                                model.predict_proba(model_df_test[model_df.columns])[:, 1])))

    logger.info('apps with variance > 0.09 + remove apps by pre installed ; devices models with variance > 0.0099')
    model_df = X_train.copy()
    model_df_test = X_test.copy()
    for property in ['carrier_name', 'device_model', 'device_vendor']:
        for p, apps_replace in pre_installed[property].groupby(level=0):
            apps_replace_list = apps_replace.index.get_level_values('level_1').tolist()
            model_df.loc[model_df[property] == p, apps_replace_list] = 0
            model_df_test.loc[model_df_test[property] == p, apps_replace_list] = 0
    filtered_apps = utils.filter_low_var_columns(model_df[apps], 0.09)
    filtered_models = utils.filter_low_var_columns(model_df[device_models], 0.0099)
    features = np.append(filtered_models, filtered_apps)
    model_df = model_df[features]
    model = models.logistic_regression(model_df, y_train, folds)
    logger.info('test average precision: {}'.format(
        average_precision_score(y_test,
                                model.predict_proba(model_df_test[model_df.columns])[:, 1])))

    logger.info('apps with variance > 0.09')
    filtered_apps = utils.filter_low_var_columns(X_train[apps], 0.09)
    features = filtered_apps
    model_df = X_train[features].copy()
    model = models.logistic_regression(model_df, y_train, folds)
    logger.info('test average precision: {}'.format(
        average_precision_score(y_test,
                                model.predict_proba(X_test[model_df.columns])[:, 1])))

    logger.info('apps with variance > 0.09 + remove apps by pre installed')
    model_df = X_train.copy()
    model_df_test = X_test.copy()
    for property in ['carrier_name', 'device_model', 'device_vendor']:
        for p, apps_replace in pre_installed[property].groupby(level=0):
            apps_replace_list = apps_replace.index.get_level_values('level_1').tolist()
            model_df.loc[model_df[property] == p, apps_replace_list] = 0
            model_df_test.loc[model_df_test[property] == p, apps_replace_list] = 0
    filtered_apps = utils.filter_low_var_columns(model_df[apps], 0.09)
    features = filtered_apps
    model_df = model_df[features]
    model = models.logistic_regression(model_df, y_train, folds)
    logger.info('test average precision: {}'.format(
        average_precision_score(y_test,
                                model.predict_proba(model_df_test[model_df.columns])[:, 1])))


def main():
    args = parse_args()
    logger = soluto_log.get_logger(filename='photography_model')

    try:
        logger.info('start')
        X_train, X_test, y_train, y_test = utils.load_ready_train_test(args.data_path)
        models_variations(X_train, X_test, y_train, y_test, args.folds)
    except Exception as e:
        logger.exception('Exception')
    finally:
        logger.info('end')


if __name__ == '__main__':
    main()
