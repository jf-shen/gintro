from chinese_calendar import is_workday
from datetime import datetime
from enum import Enum


def is_trade_day(date):
    # 检查日期是否是工作日且不是周末
    if is_workday(date) and date.weekday() < 5:  # 周一到周五
        return True
    return False


class Log(Enum):
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3


class Logger:
    def __init__(self, log_level=Log.INFO):
        self.log_level = Log(log_level)

    def print(self, msg, log_level=Log.INFO):
        if Log(log_level).value >= self.log_level.value:
            print(msg)

    def debug(self, msg):
        self.print('[DEBUG] ' + msg, log_level=Log.DEBUG)

    def info(self, msg):
        self.print('[INFO] ' + msg, log_level=Log.INFO)

    def warn(self, msg):
        self.print('[WARNING]' + msg, log_level=Log.WARNING)

    def error(self, msg):
        self.print('[ERROR] ' + msg, log_level=Log.ERROR)



