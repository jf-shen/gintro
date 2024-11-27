import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd
import time
import os

from google.protobuf.service import RpcException

import gintro.date as gd
from gintro import timeit
from .log import Logger


# get stock pool
def get_stock_pool():
    df_stock_sh = ak.stock_sh_a_spot_em()
    df_stock_sh['exchange'] = 'sh'
    df_stock_sz = ak.stock_sz_a_spot_em()
    df_stock_sz['exchange'] = 'sz'
    df_stock = pd.concat([df_stock_sh, df_stock_sz])
    # df_stock = df_stock[~df_stock['最新价'].isna()]  #  开盘前通过akshare获取的价格可能都是NA
    return df_stock


class DailyHistUpdater:
    def __init__(self,
                 path,
                 end_date=None):

        self.path = path   # 所有文件相关的操作在该目录下进行
        self.data_path = os.path.join(path, 'data')
        self.status_path = os.path.join(path, 'status')

        os.makedirs(self.data_path, exist_ok=True)
        os.makedirs(self.status_path, exist_ok=True)

        # end_date are not allowed to reset
        self.end_date = gd.today() if (end_date is None) else end_date
        self.status_file = os.path.join(self.status_path, self.end_date)  # 存储已经执行成功的stock code

        # data files
        self.succ_list = []

        # multi-threading
        self.file_lock = threading.Lock()

        # log
        self.logger = Logger()
        self.print_gap = 3
        self.total_num = -1
        self.verbose = False  # 是否打印每只股票的log


    def log_level(self, level):
        self.logger.set_log_level(level)

    def get_succ_list(self):
        succ_list = []
        if os.path.exists(self.status_file):
            with open(self.status_file, 'r') as f:
                data = f.read()
                succ_list = data.strip().split('\n')
                succ_list = list(set(succ_list))
                self.logger.info('len(succ_list) = %i' % len(succ_list))
                f.close()
        else:
            self.logger.warn(f"status file doesn't exists: {self.status_file}")
        return succ_list


    def download_daily_hist(self, row, i):
        logger = self.logger
        start_time = time.time()
        code = row['代码']
        exchange = row['exchange']
        name = row['名称']
        if code in self.succ_list:
            logger.debug(f'{name}: {code} is succ, download next')
            return -1

        logger.debug(f'[{i + 1}/{self.total_num}] start downloading {name}: {code}')
        symbol = exchange + code
        df = ak.stock_zh_a_hist_tx(
            symbol=symbol,
            end_date=self.end_date,
            adjust="qfq"
        )
        df['code'] = code
        df['名称'] = name
        df['exchange'] = exchange
        save_path = f"{self.data_path}/{code}.csv"

        logger.debug(f'[{i + 1}/{self.total_num}] save to path = {save_path}')
        df.to_csv(f'{self.data_path}/{code}.csv', encoding='utf_8_sig')

        # 使用锁来同步写入操作
        with self.file_lock:
            with open(self.status_file, 'a+') as fp:
                fp.write(code + '\n')
                fp.flush()
                fp.close()

        logger.debug(f'[{i + 1}/{self.total_num}] finish downloading {name}: {code}, time elapsed = %.2f' %
              (time.time() - start_time))

        return 1


    def update_daily_hist(self, row, i):
        # TODO: 判断start_date和end_date之间是否是trade_day，如果不是直接跳过查询
        start_time = time.time()
        logger = self.logger

        code = row['代码']
        exchange = row['exchange']
        name = row['名称']

        if code in self.succ_list:
            logger.debug(f'{name}: {code} is succ, update next')
            return -1

        file_name = f'{code}.csv'
        save_path = f'{self.data_path}/{code}.csv'

        df = None
        if file_name in os.listdir(self.data_path):
            df = pd.read_csv(save_path, index_col=0)
            max_date = df['date'].max()
            start_date = gd.date_plus(max_date.replace('-', ''), 1)
        else:
            start_date = '19900101'

        end_date = self.end_date

        if start_date > end_date:
            logger.warn(f"skip {code}.{exchange} since start_date >= end_date: "
                        f"start_date = {start_date}, end_date = {end_date}")
            return

        logger.debug(f'[{i + 1}/{self.total_num}] start updating {name}: {code}, '
                     f'date_range = [{start_date} ~ {end_date}]')
        symbol = exchange + code
        try:
            df_incr = ak.stock_zh_a_hist_tx(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
        except Exception as e:
            raise RpcException(f'[akshare error] symbol = {symbol}, start_date = {start_date}, '
                               f'end_date = {end_date}, Exception = {e}')

        df_incr['code'] = code
        df_incr['名称'] = name
        df_incr['exchange'] = exchange

        if df is None:   # data file not found
            df = df_incr
        else:
            df = pd.concat([df, df_incr], axis=0)

        logger.debug(f'[{i + 1}/{self.total_num}] save to path = {save_path}')
        df.to_csv(save_path, encoding='utf_8_sig')

        # 使用锁来同步写入操作
        with self.file_lock:
            with open(self.status_file, 'a+') as fp:
                fp.write(code + '\n')
                fp.flush()
                fp.close()

        logger.debug(f'[{i + 1}/{self.total_num}] finish downloading {name}: {code}, '
                     f'time elapsed = %.2fs' % (time.time() - start_time))
        return 1


    @timeit
    def process(self, df, fn):
        start_time = time.time()
        last_print_time = start_time

        process_num = 0
        self.total_num = df.shape[0]
        self.succ_list = self.get_succ_list()

        for i, row in df.iterrows():
            result = fn(row, i)
            if result != -1:
                process_num += 1
                time_per_item = (time.time() - start_time) / process_num

                # 隔一段时间打印日志
                if time.time() - last_print_time > self.print_gap:
                    self.logger.info(f'[{fn.__name__}] process_num = {process_num}, '
                                     f'time_per_item = %.2f' % time_per_item)
                    last_print_time = time.time()


    @timeit
    def multi_process(self, df, fn, max_workers=10):
        start_time = time.time()
        last_print_time = start_time
        logger = self.logger

        self.total_num = df.shape[0]
        self.succ_list = self.get_succ_list()
        process_num = 0

        logger.info('multi-thread mode, worker = %i' % max_workers)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fn, row, i) for i, row in df.iterrows()]
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result != -1:
                        process_num += 1
                        time_per_item = (time.time() - start_time) / process_num
                        if time.time() - last_print_time > self.print_gap:
                            logger.info(f'process_num/total_num = {process_num}/{self.total_num}, '
                                        f'time_per_item = {time_per_item}')
                            last_print_time = time.time()
                except Exception as exc:
                    logger.error(f"发生异常: {exc}")


    def update(self, df, workers=1):
        # worker = 10, stock_num = 907, days = 1, time = 11.4 min
        # worker = 1, stock_num = 864, days = 1, time = 16.59 min
        # worker = 1, stock_num = 880, days =
        if workers > 1:
            self.multi_process(df, fn=self.update_daily_hist)
        else:
            self.process(df, fn=self.update_daily_hist)


    def download(self, df, workers=1):
        if workers > 1:
            self.multi_process(df, fn=self.download_daily_hist)
        else:
            self.process(df, fn=self.download_daily_hist)


# process(df_stock, fn=update_daily_hist)
# multi_process(df_stock, fn=update_daily_hist, max_workers=10)


