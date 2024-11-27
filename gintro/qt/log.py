from enum import Enum
import os


class Log(Enum):
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3


class Logger:
    def __init__(self, log_level=Log.INFO, file=None):
        self.log_level = Log(log_level)
        self.file = file

    def set_log_level(self, log_level):
        self.log_level = Log(log_level)

    def set_file(self, path):
        os.makedirs(path, exist_ok=True)
        self.file = open(path, 'a+')

    def print(self, msg, log_level=Log.INFO):
        if Log(log_level).value >= self.log_level.value:
            print(msg)
        if self.file is not None:
            print(msg, self.file)

    def debug(self, msg):
        self.print('[DEBUG] ' + msg, log_level=Log.DEBUG)

    def info(self, msg):
        self.print('[INFO] ' + msg, log_level=Log.INFO)

    def warn(self, msg):
        self.print('[WARNING]' + msg, log_level=Log.WARNING)

    def error(self, msg):
        self.print('[ERROR] ' + msg, log_level=Log.ERROR)



