import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StructType, StructField, StringType, LongType
import argparse
from itertools import combinations
from pyspark import SparkContext, HiveContext
from pyspark_utils.pyspark_general_utils import get_unique_values_from_column

### based on the following framework ->
### https://docs.google.com/presentation/d/1Au_QpgUbEmVrj44ygZulchb8OgO_iASEsEg741SWgjA/edit?usp=sharing

def generate_empty_dataframe(sqlContext, sc):
    schema = StructType([StructField('message_type', StringType(), True),
                         StructField('sum', LongType(), True),
                         StructField('count', LongType(), False),
                         StructField('engagement_ratio', FloatType(), True)
                         ]
                        )
    return sqlContext.createDataFrame(sc.emptyRDD(), schema)


# TODO: add support for user seperation of precentiles and not only median
def seperate_users_by_docs(data, d1, d2, metric):
    '''Seperating users into 2 groups: 1- users who liked  d1 and disliked d2 and vice versa'''
    data_filtered = data.filter(data.message_type.isin([d1, d2]))
    data_pivoted = data_filtered.groupBy('device_id', 'median_' + metric).pivot('message_type').agg(F.max(metric))
    data_pivoted_median_test = data_pivoted \
        .withColumn(d1 + '_' + metric, F.when(data_pivoted[d1] > data_pivoted['median_' + metric], 1).otherwise(0)) \
        .withColumn(d2 + '_' + metric, F.when(data_pivoted[d2] > data_pivoted['median_' + metric], 1).otherwise(0)) \
        .drop(d1, d2, 'median_' + metric)
    users_bigger_than_median = data_pivoted_median_test.groupBy('device_id') \
        .agg(F.max(d1 + '_' + metric), F.max(d2 + '_' + metric)) \
        .withColumnRenamed('max({}_{})'.format(d1, metric), d1) \
        .withColumnRenamed('max({}_{})'.format(d2, metric), d2)
    group_1_users = users_bigger_than_median.where((users_bigger_than_median[d1] > 0) &
                                                   (users_bigger_than_median[d2] == 0)) \
        .drop(d1, d2)
    group_2_users = users_bigger_than_median.where((users_bigger_than_median[d2] > 0) &
                                                   (users_bigger_than_median[d1] == 0)) \
        .drop(d1, d2)
    return group_1_users, group_2_users


def generate_tuple_permutations(list_of_dims):
    perm = []
    i = 0
    lhs = list_of_dims.copy()
    for f1 in list_of_dims:
        if len(lhs) > 0:
            for f2 in lhs:
                if f1 != f2:
                    perm.append((f1, f2))
            lhs.remove(f1)
    return perm


devide_columns = F.udf(lambda col1, col2: col1 / col2, FloatType())


def calculate_engaged_ratio(users_df, data, doc, metric):
    '''Calculate engagement ratio on users list - doc:
     success = 1 when metric value is higher than docs median'''
    doc_data = data.filter(data.message_type.isin([doc]))
    filtered_dataset = users_df.join(doc_data, 'device_id').select(
        ['message_type', 'device_id', 'median_' + metric, metric])
    doc_counts = filtered_dataset.withColumn(metric + '_test',
                                             F.when(filtered_dataset[metric] > filtered_dataset['median_' + metric],
                                                    1)
                                             .otherwise(0)).groupBy('message_type') \
        .agg({'device_id': "count", metric + '_test': "sum"}) \
        .withColumnRenamed('sum({}_test)'.format(metric), 'sum') \
        .withColumnRenamed('count({})'.format('device_id'), 'count')
    ratio_df = doc_counts.withColumn('engagement_ratio', devide_columns(F.col('sum'), F.col('count')))
    return ratio_df


def out_of_sample_test(sc, sqlContext, in_sample_d1, in_sample_d2, group_1_users, group_2_users, data, metric,
                       documents):
    '''creates an out of sample engagement ratio test (doc3 , doc4...) on users who saw doc1 & doc2'''
    out_of_sample_docs = [d for d in documents if d not in [in_sample_d1, in_sample_d2]]
    out_of_sample_separation_tests_df = generate_empty_dataframe(sqlContext, sc)
    for doc in out_of_sample_docs:
        group_1_ratio = calculate_engaged_ratio(group_1_users, data, doc, metric)
        group_2_ratio = calculate_engaged_ratio(group_2_users, data, doc, metric)
        out_of_sample_separation_tests_df = out_of_sample_separation_tests_df.union(group_1_ratio).union(group_2_ratio)
    return out_of_sample_separation_tests_df


def read_data_from_s3(sqlContext, path):
    return sqlContext.read.csv(path, header=True)


def run_separation_tests(sc, sqlContext, metric, data):
    '''create seperation tests on all users and all docs on a single metric'''
    data.cache()
    documents = get_unique_values_from_column(data, "message_type")
    out_of_sample_separation_tests_df = generate_empty_dataframe(sqlContext, sc)
    for d1, d2 in combinations(documents, 2):
        group_1_users, group_2_users = seperate_users_by_docs(data, d1, d2, metric)
        separation_test_df = out_of_sample_test(sc, sqlContext, d1, d2, group_1_users, group_2_users,
                                                data, metric, documents)
        out_of_sample_separation_tests_df = out_of_sample_separation_tests_df.union(separation_test_df)
    return out_of_sample_separation_tests_df


def get_parameters():
    parser = argparse.ArgumentParser()
    parser.add_argument('--metric_list', required=True,
                        help='metric list (eg, time_on_page,scroll_depth) to run separation tests')
    parser.add_argument('--target_path', required=True, help='Target path to push files on s3')
    parser.add_argument('--data_location', required=True, help='data_location path to pulls files on s3')
    parser.add_argument('--external_lib', required=True, action='append', help='External Lib')

    args = parser.parse_args()

    metric_list = args.metric_list.split(",")
    data_location = args.data_location
    target_path = args.target_path
    external_lib = args.external_lib

    return metric_list, data_location, target_path, external_lib


if __name__ == "__main__":
    (metric_list, data_location, target_path, external_lib) = get_parameters()

    sc = SparkContext(appName="Engagement metric - separation builder", pyFiles=external_lib)
    sqlContext = HiveContext(sc)
    data = read_data_from_s3(sqlContext, data_location)

    for metric in metric_list:
        metric_separation_test = run_separation_tests(sc, sqlContext, metric, data)
        metric_separation_test.write.csv(target_path + '/{}'.format(metric), header=True)
        print('separation tests data was written for {} metric, data pushed to: {}'.format(metric,
                                                                                           target_path + '/' + metric))
    sc.stop()
