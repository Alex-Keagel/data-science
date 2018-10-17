from math import log
import numpy as np
from scipy.stats import norm
from scipy.misc import factorial


# Calculate bounded logloss: INPUT -> p: our prediction , y: label; OUTPUT -> logarithmic loss of p given y
def log_loss(p, y):
    p = max(min(p, 1. - 10e-15), 10e-15)
    return -log(p) if y == 1. else -log(1. - p)


ALPHA = 0.05


# margin of error for a sample proportion:
# q - sample proportion (the number in the sample with the characteristic of interest, divided by N - e.g - openRate)
# N - Sample size
# alpha - significance level
def calculate_error(q, N, alpha):
    z = norm.ppf(1 - alpha / 2)
    sigma = np.sqrt(q * (1 - q))
    error = sigma * z / np.sqrt(N)
    return float(error)


# likelihood function for a binomial distribution
# n: [int] the number of experiments
# x: [int] the number of successes
# theta: [float] the proposed probability of success
def binomial_likelihood(theta, n, x):
    return (factorial(n) / (factorial(x) * factorial(n - x))) \
           * (theta ** x) * ((1 - theta) ** (n - x))
