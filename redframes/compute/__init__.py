import numpy as np


class compute:
    @classmethod
    def count(cls, X):
        return len(X)

    @classmethod
    def sum(cls, X):
        return np.sum(X)

    @classmethod
    def max(cls, X):
        return np.max(X)

    @classmethod
    def mean(cls, X):
        return np.mean(X)

    @classmethod
    def median(cls, X):
        return np.median(X)

    @classmethod
    def min(cls, X):
        return np.min(X)

    @classmethod
    def std(cls, X):
        return np.std(X)
