import logging

import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.svm import l1_min_c
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import average_precision_score


def logistic_regression(model_df, response, folds):
    logger = logging.getLogger('log')
    logger.info('dataset shape: {}'.format(model_df.shape))

    response = response[model_df.index.intersection(response.index)]
    min_c = l1_min_c(model_df, response, loss='log')
    tuned_parameters = {'C': np.log10(np.logspace(min_c, min_c * 5000, 50))}

    clf = GridSearchCV(LogisticRegression(penalty='l1', random_state=100),
                       tuned_parameters, cv=folds, scoring=('neg_log_loss', 'average_precision'),
                       return_train_score=True, refit='average_precision')
    clf.fit(model_df, response)

    logger.info('CV average precision: {}'.format(clf.best_score_))
    logger.info('best param: {}'.format(clf.best_params_))
    # make sure that best index isn't on the edges of the grid
    logger.debug('best param index: {}'.format(clf.best_index_))

    logger.debug('mean train score:\n{}'.format(clf.cv_results_['mean_train_average_precision']))
    logger.debug('mean test score:\n{}'.format(clf.cv_results_['mean_test_average_precision']))

    coefs = pd.DataFrame(list(zip(model_df.columns, clf.best_estimator_.coef_[0])),
                         columns=['app', 'coef'])
    logger.info('train features after regularization: {}'.format((coefs['coef'] != 0).sum()))
    logger.debug('coefficients:\n{}'.format(
        coefs[coefs['coef'] != 0].sort_values('coef', ascending=False).to_string(index=False)))
    logger.debug('intercept: {}'.format(clf.best_estimator_.intercept_[0]))
    logger.info('train average precision: {}'.format(
        average_precision_score(response, clf.best_estimator_.predict_proba(model_df)[:, 1])))

    return clf.best_estimator_
