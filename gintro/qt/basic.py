from chinese_calendar import is_workday
from datetime import datetime
import pandas as pd


def is_trade_day(date):
    # 检查日期是否是工作日且不是周末
    if is_workday(date) and date.weekday() < 5:  # 周一到周五
        return True
    return False


def shift_join(df, fields, shift, shift_by='date', prefix=None):
    """
    :param df: a pandas data frame
    :param fields: a list
    :param shift: int
    :param shift_by: a column in df
    :param prefix: prefix for names of shifted fields
    :return: df with shifted field
    """
    df = df.sort_values(by=shift_by, ascending=True)
    df['index'] = list(range(df.shape[0]))
    dff = df[fields + ['index']].copy()
    dff['index'] = dff['index'] - shift

    def _rename(field):
        if prefix is None:
            return f'{field}_{shift}'
        else:
            return f'{prefix}_{field}_{shift}'

    rename_dict = {field: _rename(field) for field in fields}
    dff = dff.rename(columns=rename_dict)
    df = df.merge(dff, how='left', on='index')
    return df


