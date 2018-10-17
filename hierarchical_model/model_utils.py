from datetime import datetime, timedelta

from scipy.special import digamma

THRESHOLD = 1e-3
MAX_ITERATIONS = 900


def get_timewindow(days_back, end_date=None):
    if end_date:
        last_day = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        last_day = datetime.now() - timedelta(days=4)
    last_day = last_day.date()
    start_date = last_day - timedelta(days=days_back)
    return start_date, last_day


def exp_smoothing(value, gamma, current_date, end_date):
    lag = (end_date - current_date).days
    return gamma * (1 - gamma) ** lag * value


def calc_alpha_beta(trial_success, alpha, beta, iter=0):
    numerator_alpha = 0
    numerator_beta = 0
    denominator = 0

    for i in trial_success:
        if alpha > 0:
            numerator_alpha += digamma(i['success'] + alpha) - digamma(alpha)
        numerator_beta += digamma(i['trial'] - i['success'] + beta) - digamma(beta)
        denominator += digamma(i['trial'] + alpha + beta) - digamma(alpha + beta)

    if alpha > 0:
        alpha_new = alpha * (numerator_alpha / denominator)
    else:
        alpha_new = alpha
    beta_new = beta * (numerator_beta / denominator)

    if (((abs(alpha - alpha_new) < THRESHOLD) and (abs(beta - beta_new) < THRESHOLD))
            or iter == MAX_ITERATIONS):
        # convert from numpy.float64
        return float(alpha_new), float(beta_new)

    return calc_alpha_beta(trial_success, alpha_new, beta_new, iter + 1)


def update_dict_rec(order, values, result):
    if len(order) == 0:
        result['probability'] = values['probability']
    else:
        if values[order[0]] not in result:
            result[values[order[0]]] = {}
        update_dict_rec(order[1:], values, result[values[order[0]]])
