from glob import glob
import os
from functools import partial

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_selection import VarianceThreshold

import converters as c


# score is not really an id, just ignored in this stage
IDS = ['device_id', 'user_id', 'mobileuid', 'subid', 'source_dfx_id', 'deviceId', 'score']
DEVICE_CAT = ['carrier_name', 'device_model', 'device_model_name', 'device_vendor']
DEVICE_NUM = ['device_os_type']
# 'nameprefix' and 'gender' are consistent
DGX_CAT = ['nameprefix', 'state', 'dwelling_type', 'professionalrollup']
DGX_NUM = ['age_range', 'income_range_vds', 'gender', 'marital_status',
           'home_ownership', 'length_of_residence', 'presence_of_children', 'home_value_range',
           'networthindicator_rollup', 'level_of_education', 'head_of_household',
           'mail_public_responder_indicator', 'mail_responsive_buyer_indicator', 'wealth_decile',
           'homeandlandvalue', 'first_mortgage_amount', 'active_fitness_interest', 'golf_interest',
           'traveler', 'green_advocate', 'premover']
RESPONSE = 'photography_interest'


def get_apps(features_list):
    not_app_columns = IDS + DEVICE_CAT + DEVICE_NUM + DGX_CAT + DGX_NUM + [RESPONSE]
    return [c for c in features_list if c not in not_app_columns]


def load_ready_train_test(dir_path, response, prefix='X'):
    X_train = pd.read_csv(os.path.join(dir_path, prefix + '_train.csv.gz'), index_col='device_id',
                          low_memory=False)
    X_test = pd.read_csv(os.path.join(dir_path, prefix + '_test.csv.gz'), index_col='device_id',
                         low_memory=False)

    return X_train, X_test, X_train[response], X_test[response]


def read_data(dir_path):
    all_files = glob(dir_path + '/*.csv*')
    df_list = []
    converters = {'device_os_type': partial(c.to_binary, true_value='ios', false_value='android'),
                  'gender': partial(c.to_binary, true_value='M', false_value='F'),
                  'marital_status': partial(c.to_binary, true_value='Married', false_value='Single'),
                  'home_ownership': partial(c.to_binary, true_value='Owner', false_value='Renter'),
                  'presence_of_children': partial(c.to_binary, true_value='Y', false_value='N'),
                  'head_of_household': partial(c.to_binary, true_value='Y', false_value='N'),
                  'premover': partial(c.to_binary, true_value='Y', false_value='N'),
                  'age_range': c.get_age,
                  'home_value_range': c.get_home_value,
                  'income_range_vds': c.get_income,
                  'networthindicator_rollup': c.get_networth,
                  'length_of_residence': c.get_length_of_residence,
                  'level_of_education': c.get_education
                  }
    for file_ in all_files:
        df = pd.read_csv(file_, index_col='device_id', low_memory=False, converters=converters)
        df_list.append(df)
    df = pd.concat(df_list)
    return df


def replace_unkown(df):
    # FIXME: use "na_values" parameter in pd.read_csv()
    columns = \
        ['user_id', 'carrier_name', 'device_model', 'device_model_name', 'device_vendor',
         'nameprefix', 'state', 'dwelling_type', 'professionalrollup']
    replacement = {'.*Unknown.*': None, '': None}
    return df.replace(regex=dict(zip(columns, [replacement]*len(columns))))


def create_test_train(dir_path, test_size):
    original_df = read_data(dir_path)
    original_df = replace_unkown(original_df)

    # remove devices that don't have a response
    df = original_df[~original_df[RESPONSE].isnull()]

    apps = get_apps(df.columns)

    # rtemove devices without apps data
    df = df[df[apps].isnull().sum(axis=1) == 0]

    # remove apps columns that are all zeros
    df = df.drop(df[apps].columns[df[apps].sum() == 0], axis=1)

    X_train, X_test, y_train, y_test = \
        train_test_split(df, df[RESPONSE], test_size=test_size, random_state=100,
                         stratify=df[RESPONSE])

    # DataFrame to Series
    y_train = y_train.squeeze()
    y_test = y_test.squeeze()

    return X_train, X_test, y_train, y_test


def get_top_by_property(df, column, min_occurrence, top_cutoff):
    '''
    for each value of 'column' with at least 'min_occurrence' observations, return the apps that
    appear in more than 'top_cutoff' percent of devices on that level
    '''

    # number of observations for each value
    property_counts = df[column].value_counts()
    stack_apps_by_property = df.set_index(column).stack().reset_index()
    # number of devices that have an app installed, by device property and app
    property_apps = stack_apps_by_property[stack_apps_by_property[0] == 1] \
        .groupby(column)['level_1'].value_counts()
    normalized_model_app = property_apps.divide(property_counts, level=column)

    top_property_counts = \
        normalized_model_app[property_counts[property_counts > min_occurrence].index.tolist()]
    return top_property_counts[top_property_counts > top_cutoff]


def filter_low_var_columns(df, threshold):
    low_var = VarianceThreshold(threshold=threshold).fit(df)
    return df.columns[low_var.get_support()]
