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
    WARNING = 5
    ERROR = 10


class Logger:
    def __init__(self, log_level=Log.INFO):
        self.log_level = log_level

    def info(self, msg, log_level=Log.INFO):
        if self.log_level >= log_level:
            print(msg)

    def debug(self, msg):
        self.info(msg, log_level=Log.DEBUG)

    def warn(self, msg):
        self.info(msg, log_level=Log.WARNING)

    def error(self, msg):
        self.info(msg, log_level=Log.ERROR)



