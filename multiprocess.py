import multiprocessing
from sqlalchemy import create_engine
import pandas as pd
import datetime
import dbfuncs
import math
import time
from pandas.tseries.offsets import BDay
import numpy as np

class Multiprocess_Sql():
    def __init__(self,
                 today = datetime.datetime.today(),
                 lookbackperiod = 0,
                 process_num = 8,
                 code_list = []):

        self.res_queue = multiprocessing.Queue()
        self.today = today
        self.lookbackperiod = lookbackperiod
        self.process_num = process_num
        self.code_list = code_list

    def worker(self, a,b,code_list):
        sql = "select s_info_windcode,trade_dt,b_anal_yield_cnbd,b_anal_credibility from wind.cbondanalysiscnbd t where s_info_windcode in {0} and trade_dt between '{1}' and '{2}'".format(
            tuple(code_list), a, b)
        res = dbfuncs.from_sql_orcl_manually(sql, DBConf = "Wind")
        self.res_queue.put(res)

    def multiprocess_getdata(self):
        today = self.today
        lookbackperiod = self.lookbackperiod
        process_num = self.process_num
        code_list = self.code_list

        results = pd.DataFrame()
        task_list = []
        date_range = pd.bdate_range(end=today, periods = process_num+1, freq = '{0}B'.format(math.ceil(lookbackperiod/process_num)))
        for i in range(len(date_range)-1):
            start = date_range[i].strftime('%Y%m%d')
            end = date_range[i+1].strftime('%Y%m%d')

            p_task = multiprocessing.Process(target = self.worker, args = (start, end, code_list))
            task_list.append(p_task)
            p_task.start()

        get_queue_num = 0
        while True:
            if get_queue_num == 0:
                results = self.res_queue.get()
            else:
                results = pd.concat([results, self.res_queue.get()])
            get_queue_num += 1
            if get_queue_num == process_num:
                break

        for task in task_list:
            task.join()

        results.drop_duplicates(inplace = True, ignore_index = True)
        return results

    def retrieve_data( self,
                       df,
                       metrics,
                       subsets):
        i = 0
        for metric in metrics:
            try:
                df = df[df[metric] == subsets[i]]
            except AttributeError:
                raise ('没找到{0}在{1}中'.format(metric, subsets[i]))
            i += 1

        try:
            return df.B_ANAL_YIELD_CNBD.iloc[0]
        except IndexError:
            return np.nan

    def mp_getdata( self ):
        today = self.today
        lookbackperiod = self.lookbackperiod
        process_num = self.process_num
        code_list = self.code_list

        results = pd.DataFrame()
        task_list = []
        date_range = pd.bdate_range(end = today, periods = process_num + 1,
                                    freq = '{0}B'.format(math.ceil(lookbackperiod / process_num)))
        for i in range(len(date_range) - 1):
            start = date_range[i].strftime('%Y%m%d')
            end = date_range[i + 1].strftime('%Y%m%d')

            p_task = multiprocessing.Process(target = self.worker, args = (start, end, code_list))
            task_list.append(p_task)
            p_task.start()

        get_queue_num = 0
        while True:
            if get_queue_num == 0:
                results = self.res_queue.get()
            else:
                results = pd.concat([results, self.res_queue.get()])
            get_queue_num += 1
            if get_queue_num == process_num:
                break

        for task in task_list:
            task.join()
        # date_pairs = zip(date_range[:-1], date_range[1:], itertools.repeat(code_list))
        # data = p.starmap(self.worker, date_pairs)
        #
        # p.close()
        # p.join()
        #
        # results = pd.concat(data)
        results.drop_duplicates(inplace = True, ignore_index = True)

        return results


if __name__ == '__main__':


    issuer = '津城建'
    lookbackperiod = 300
    output = pd.DataFrame(columns = list(range(2,13)))
    for process_num in range(2,13):
        totaltime = []
        for i in range(10):
            start = time.time()
            # process_num = 8

            connect = create_engine('mysql+pymysql://houyuhui:UYFnnrAFl9yTbqf3@172.22.213.219:3306/bondtrade')
            sql_1 = 'select bond_code, sec_name, redemption_beginning, maturitydate, issue_firstissue, adj_rate_latestmir_cnbd from bondinfo where sec_name like "__{0}%%";'.format(
                    issuer)
            temp_1 = pd.read_sql(sql_1, connect)
            code_list = temp_1.bond_code
            today = (datetime.datetime.now().date() - BDay(1)).date()


            # p = multiprocessing.Pool(processes=process_num) # process num is recommended to be you cpu core num
            # results = pd.DataFrame()
            # task_list = []
            # date_range = pd.bdate_range(end = today, periods = process_num + 1,
            #                             freq = '{0}B'.format(math.ceil(lookbackperiod / process_num)))
            # for i in range(len(date_range) - 1):
            #     start = date_range[i].strftime('%Y%m%d')
            #     end = date_range[i + 1].strftime('%Y%m%d')
            #
            #     p_task = multiprocessing.Process(target = worker, args = (start, end, code_list))
            #     task_list.append(p_task)
            #     p_task.start()
            #
            # get_queue_num = 0
            # while True:
            #     if get_queue_num == 0:
            #         results = res_queue.get()
            #     else:
            #         results = pd.concat([results, res_queue.get()])
            #     get_queue_num += 1
            #     if get_queue_num == process_num:
            #         break
            #
            # for task in task_list:
            #     task.close()
            #     task.join()
            # res_queue = multiprocessing.Queue()
            p = multiprocess_sql(today,lookbackperiod,process_num,code_list)
            results = p.multiprocess_getdata()

            # p = multiprocessing.Pool(processes = process_num)  # process num is recommended to be you cpu core num
            #
            # date_range = pd.bdate_range(end = today, periods = process_num + 1,
            #                             freq = '{0}B'.format(math.ceil(lookbackperiod / process_num)))
            #
            # date_pairs = zip(date_range, date_range[1:],itertools.repeat(code_list))
            #
            # data = p.starmap(worker, date_pairs)
            #
            # p.close()
            # p.join()
            #
            # results = pd.concat(data)
            # results.drop_duplicates(inplace = True, ignore_index = True)
            #
            end = time.time()
            totaltime.append(end-start)
        output.loc[0,process_num] = np.mean(totaltime)
    print(output)
    # test = results[results.TRADE_DT > '20220101']
    pass