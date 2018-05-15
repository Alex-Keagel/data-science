from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql import functions as F
from pyspark.sql.functions import col, countDistinct
from pyspark.mllib.evaluation import BinaryClassificationMetrics as metric

from pyspark_utils.math_utils import log_loss


def get_sum_of_column(df, columnName):
    return int(df.rdd.map(lambda x: float(x[columnName])).reduce(lambda x, y: x + y))


def string_indexers(features):
    string_indexers = [StringIndexer(inputCol=x, outputCol="idx_{0}".format(x))
                       for x in features]
    return string_indexers


def encoder(features):
    encoders = [OneHotEncoder(inputCol="idx_{0}".format(x), outputCol="enc_{0}".format(x))
                for x in features]
    return encoders


def vector_assembler(features, prefix):
    assembler = VectorAssembler(
        inputCols=["{0}_{1}".format(prefix, x) for x in features],
        outputCol="features")
    return assembler


# Apply string indexers on data
def get_indexed_data(DataDF, string_indexers):
    pipeline = Pipeline(stages=string_indexers)
    model = pipeline.fit(DataDF)
    indexed = model.transform(DataDF)
    return indexed


# Apply encoding data
def get_encoded_data(DataDF, encoders):
    pipeline = Pipeline(stages=encoders)
    model = pipeline.fit(DataDF)
    encoded = model.transform(DataDF)
    return encoded


# Apply vector assembler on data
def get_vector_essemblered_data(DataDF, assembler):
    pipeline = Pipeline(stages=[assembler])
    model = pipeline.fit(DataDF)
    vector_essemblered = model.transform(DataDF)
    return vector_essemblered


# Creates a dictionary of categorical features and cont of unique values in that category
def create_categorical_feature_map(df, categorical_features):
    temp_data = df.agg(*(countDistinct(col(c)).alias(c) for c in df.columns)).collect()[0]
    data_map = {}
    for feature_indx in range(0, len(df.columns)):
        if temp_data.__fields__[feature_indx] in categorical_features:
            data_map[feature_indx] = int(temp_data[feature_indx])
    return data_map


# downsampling method
def get_balanced_data_set(rdd_data_set, num_of_observations):
    rdd_balanced = rdd_data_set.filter(lambda lp: lp.label == 1) \
        .toDF().limit(num_of_observations) \
        .unionAll(rdd_data_set.filter(lambda lp: lp.label == 0).toDF().limit(num_of_observations)) \
        .rdd
    return rdd_balanced


# Creates a minority category value if the the total count of events per category is lower than the threshold
def convert_to_minority_If_needed(df, category, count_threshold):
    count_col = df.groupBy(category).count()
    df = df.join(count_col, category, "inner")
    return df.withColumn(category + '_reduced',
                         F.when(df['count'] < count_threshold, 'MinorityCategory').otherwise(df[category])) \
        .drop('count') \
        .drop(category) \
        .withColumnRenamed(category + '_reduced', category)


def get_unique_values_from_column(data, column_name):
    return data.select(column_name).distinct().rdd.flatMap(lambda x: x).collect()


def get_stdDev_from_column(data, column):
    return data.agg({column: "stddev"}).collect()[0][0]


def get_mean_from_column(data, column):
    return data.agg({column: "mean"}).collect()[0][0]


# Calculates min observations for statistical significance level alpha
def get_min_observations(data, alpha, sample_proportion_column_name):
    from scipy.stats import norm
    z = norm.ppf(1 - alpha / 2)
    sdtDev = get_stdDev_from_column(data, sample_proportion_column_name)
    return int(z * z * sdtDev * (1 - sdtDev) / (alpha * alpha))


# Converts string true/false to Boolean True/False
def str_to_bool(s):
    if s.lower() == 'true':
        return True
    elif s.lower() == 'false':
        return False
    else:
        raise ValueError("Cannot covert {} to a bool".format(s))


# Creates a prediction label tuple
def get_score_and_label_tuples(model, test_data):
    prediction = model.predict(test_data.map(lambda x: x.features))
    labels = test_data.map(lambda x: x.label)
    return prediction.zip(labels)


# Calculates AUC score
def get_auc_score(scores_and_labels_data_set):
    return metric(scores_and_labels_data_set).areaUnderROC


# Calculates calibration ratio : sum(p)/sum(y)
def get_calibration_ratio(scores_and_labels_data_set):
    predictions = scores_and_labels_data_set.map(lambda x: x[0]).sum()
    labels = scores_and_labels_data_set.map(lambda x: x[1]).sum()
    return float(predictions / labels)


# Calculates MSE
def get_mse(scores_and_labels_data_set):
    return scores_and_labels_data_set.map(lambda p_y: (p_y[1] - p_y[0]) * (p_y[1] - p_y[0])).sum() / float(
        scores_and_labels_data_set.count())


# Calculates log-loss
def get_log_loss(scores_and_labels_data_set):
    return scores_and_labels_data_set.map(lambda p_y: log_loss(p_y[0], p_y[1])).sum() / float(
        scores_and_labels_data_set.count())
