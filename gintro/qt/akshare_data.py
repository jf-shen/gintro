import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd
import time
import os
import gintro.date as gd
from gintro import timeit


# get stock pool
def get_stock_pool():
    df_stock_sh = ak.stock_sh_a_spot_em()
    df_stock_sh['exchange'] = 'sh'
    df_stock_sz = ak.stock_sz_a_spot_em()
    df_stock_sz['exchange'] = 'sz'
    df_stock = pd.concat([df_stock_sh, df_stock_sz])
    # df_stock = df_stock[~df_stock['最新价'].isna()]  #  开盘前通过akshare获取的价格可能都是NA
    return df_stock


class DailyHistDownloader:
    def __init__(self,
                 path,
                 end_date=None):

        self.path = path
        self.data_path = os.path.join(path, 'data')
        self.status_path = os.path.join(path, 'status')

        # set value in self.set_end_date()
        self.end_date = None
        self.status_file = None
        self.set_end_date(end_date)

        # data files
        self.data_files = os.listdir(self.data_path)

        self.time_lag = 3
        self.total_num = -1


    def set_end_date(self, end_date):
        self.end_date = gd.today() if (end_date is None) else end_date
        self.status_file = os.path.join(self.status_path, self.end_date)


    def get_succ_list(self):
        succ_list = []
        if os.path.exists(self.status_file):
            with open(self.status_file, 'r') as f:
                data = f.read()
                succ_list = data.strip().split('\n')
                succ_list = list(set(succ_list))
                print('len(succ_list) = %i' % len(succ_list))
                f.close()
        else:
            print(f"status file doesn't exists: {self.status_file}")

        return succ_list


    @timeit
    def process(self, df, fn):
        start_time = time.time()
        last_print_time = start_time

        process_num = 0
        self.total_num = df.shape[0]

        for i, row in df.iterrows():
            result = fn(row, i)

            if result != -1:
                process_num += 1
                time_per_item = (time.time() - start_time) / process_num

                if time.time() - last_print_time > self.time_lag:
                    print(f'[{fn.__name__}] process_num = {process_num}, time_per_item = %.2f' % time_per_item)
                    last_print_time = time.time()


    def update_daily_hist(self, row, i):
        # TODO: 判断start_date和end_date之间是否是trade_day，如果不是直接跳过查询
        start_time = time.time()
        code = row['代码']
        exchange = row['exchange']
        name = row['名称']

        file_name = f'{code}.csv'
        save_path = f'{self.data_path}/{code}.csv'

        df = None
        if file_name in self.data_files:
            df = pd.read_csv(save_path, index_col=0)
            max_date = df['date'].max()
            start_date = gd.date_plus(max_date.replace('-', ''), 1)
        else:
            start_date = '19900101'

        end_date = self.end_date

        if start_date > end_date:
            print(f"skip {code}.{exchange} since start_date >= end_date: start_date = {start_date}, end_date = {end_date}")
            return

        print(f'[{i + 1}/{self.total_num}] start updating {name}: {code}, date_range = [{start_date} ~ {end_date}]')
        symbol = exchange + code
        df_incr = ak.stock_zh_a_hist_tx(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"
        )
        df_incr['code'] = code
        df_incr['名称'] = name
        df_incr['exchange'] = exchange

        if df is None:   # data file not found
            df = df_incr
        else:
            df = pd.concat([df, df_incr], axis=0)

        print(f'[{i + 1}/{self.total_num}] save to path = {save_path}')
        df.to_csv(save_path, encoding='utf_8_sig')

        file_lock = threading.Lock()
        # 使用锁来同步写入操作
        with file_lock:
            with open(self.status_file, 'a+') as fp:
                fp.write(code + '\n')
                fp.flush()
                fp.close()

        print(f'[{i + 1}/{self.total_num}] finish downloading {name}: {code}, time elasped = %.2fs' %
              (time.time() - start_time))


# ================= 我是分割线 ============= #

def multi_process(df_stock, fn, max_workers=10):
    print('multi-thread mode, worker = %i' % max_workers)
    start_time = time.time()
    process_num = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fn, row, i) for i, row in df_stock.iterrows()]
        for future in as_completed(futures):
            try:
                result = future.result()
                if result != -1:
                    process_num += 1
                    time_per_item = (time.time() - start_time) / process_num
                    print(f'process_num = {process_num}, time_per_item = {time_per_item}')
            except Exception as exc:
                print(f"发生异常: {exc}")


# process(df_stock, fn=update_daily_hist)
multi_process(df_stock, fn=update_daily_hist, max_workers=10)


