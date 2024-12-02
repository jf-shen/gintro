import numpy as np


class FeatureGenerator:
    """
    FG do NOT handle joining data
    """

    def __init__(self,
                 past_fields,    # = ['open', 'close', 'high', 'low', 'amount'],
                 past_dts,       # = [-i for i in range(10)][::-1],
                 future_dts,     # = [1, 2, 3, 5, 10],
                 future_fields,  # = ['open', 'close', 'high', 'low']
                ):
        self.past_fields = past_fields
        self.future_fields = future_fields
        self.past_dts = past_dts
        self.future_dts = future_dts

        self.fea_tuple = None


    def make_label_plan(self):
        future_dts = self.future_dts
        fields = self.future_fields
        label_tuple = []
        for dt in future_dts:
            for field in fields:
                base_name = f'{field}_0'
                val_name = f'{field}_{dt}'
                incr_name = f'{field}_{dt}_incr'
                label_tuple.append((base_name, val_name, incr_name))
        return label_tuple

    def make_fea_plan(self):
        past_dts = self.past_dts
        fields = self.past_fields
        fea_tuple = []
        for last_dt, dt in zip(past_dts[:-1], past_dts[1:]):
            for field in fields:
                base_name = f'{field}_{last_dt}'
                val_name = f'{field}_{dt}'
                incr_name = f'd_{field}_{dt}'
                fea_tuple.append((base_name, val_name, incr_name))
        return fea_tuple

    def generate_feature(self, df):
        res = df[['date', 'code']].copy()
        label_tuple = self.make_label_plan()
        fea_tuple = self.make_fea_plan()
        for base_name, val_name, incr_name in label_tuple + fea_tuple:
            res[incr_name] = df[val_name] / df[base_name] - 1
        return res
