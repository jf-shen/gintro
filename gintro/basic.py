import numpy as np


def sigmoid(x):
    return 1 / (1 + np.exp(-x))


def logit(x):
    return np.log(x / (1-x))


def get_auc(pred, label):
    assert len(pred) == len(label), f'len(pred) = {len(pred)}, len(label) = {len(label)}'
    tp = [x for x, y in zip(pred, label) if y == 1]     # true positive
    fp = [x for x, y in zip(pred, label) if y == 0]     # false positive
    tp = sorted(tp)
    fp = sorted(fp)
    ordered_pair = 0
    neg_idx = 0
    for i, x in enumerate(tp):
        while neg_idx < len(fp) and fp[neg_idx] < x:
            neg_idx += 1
        ordered_pair = neg_idx * i
    return ordered_pair / (len(fp) * len(fp))




