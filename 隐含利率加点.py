import time
from sqlalchemy import create_engine
import pymysql
import pandas as pd
from pandas.tseries.offsets import BDay
import numpy as np
import datetime
import matplotlib.pyplot as plt
from scipy.interpolate import pchip_interpolate
from multiprocess import Multiprocess_Sql
import dbfuncs


### 函数部分
# 债券种类区分
# 剔除含权债
# 筛选发行品种
# 备用column字段 windl1type：短期融资券 windl2type：超短期融资债券  interesttype：固定利率

class Curve_Addon:

    def __init__(self,
                 issuer,
                 date = datetime.datetime.now().date() - BDay(1)):
        self.issuer = issuer
        self.date = date

    def edge_case( self,
                   right,
                   length):
        if right > length:
            right -= 1
            left = right - 2
        elif right == 1:
            right += 1
            left = 0
        elif right < 3:
            left = 0
        else:
            left = right - 3

        return left, right

    def get_latest_yield(self,
                         code: str or list,
                         connect = None,
                         lookbackperiod:int = 0):
        date = self.date
        if not lookbackperiod:      # 只取一天数据的时候，就不用多进程来优化
            sql = "select s_info_windcode,trade_dt,b_anal_yield_cnbd,b_anal_credibility from wind.cbondanalysiscnbd t where s_info_windcode in {0} and trade_dt='{1}'".format(
                    tuple(code), date.strftime('%Y%m%d'))
            temp = dbfuncs.from_sql_orcl_manually(sql, DBConf = "Wind")
            return temp
        else:       # 把时间分成8个部分，多进程读入
            process_num = 8
            p = Multiprocess_Sql(date, lookbackperiod, process_num, code)
            temp = p.multiprocess_getdata()
            return temp

    ## 获取单日债券隐含收益率加点情况
    def hermite_curve_daily(self,
                            term = None,
                            num = 100):
        # term：期限 -> 0.08Y
        # num：hermite插值后绘图点数量
        issuer = self.issuer
        today = date = self.date
        columns_for_bondinfo = ['bond_code', 'sec_name','redemption_beginning']

        ### 数据库抓取数据
        # 抓取个券信息列表
        connect = create_engine('mysql+pymysql://houyuhui:UYFnnrAFl9yTbqf3@172.22.213.219:3306/bondtrade')
        sql = 'select bond_code, sec_name, redemption_beginning, maturitydate, issue_firstissue, adj_rate_latestmir_cnbd from bondinfo where sec_name like "__{0}%%";'.format(issuer)
        temp = pd.read_sql(sql, connect)

        ### 数据处理
        temp = temp[(temp.maturitydate >= today)&(temp.issue_firstissue <= today)]  # 选择存续债券
        rate = temp.adj_rate_latestmir_cnbd.iloc[0]  # 获取主体评级，后续用function改进
        type = '城投债'                            # 获取主体的类别，后续用function改进
        temp = temp[columns_for_bondinfo]
        temp['ptmyear'] = temp.redemption_beginning.apply(lambda x: round(float((x-today).days)/365,3))

        # 获取存续债券给定日期的收益率
        yield_table = self.get_latest_yield(temp.bond_code) # 获取给定主体当天存续所有债券的收益率
        metrics = ['S_INFO_WINDCODE', 'TRADE_DT']
        temp['yield'] = temp.bond_code.apply(lambda x: self.retrieve_data(yield_table,metrics,[x,today.strftime('%Y%m%d')]))  # 获取存续债券给定日期的收益率

        temp.dropna(inplace=True)
        temp.sort_values(by='ptmyear', ascending=True, inplace=True)
        temp.drop_duplicates(['ptmyear'], inplace=True, ignore_index=True)  # 相同期限的债券只取其中一只
        if temp.empty:
            print('{0}{1}没有存续债券'.format(today,issuer))
            output = pd.DataFrame()
            return output

        # 获取相应评级和种类的中债关键期限收益率曲线
        connect = create_engine('mysql+pymysql://houyuhui:UYFnnrAFl9yTbqf3@172.22.213.219:3306/spreadhedge')
        sql = 'select * from {0} where bond_rank like "{1}" and trade_date like "{2}";'.format('ytm_' + type + '_1', rate,
                                                                                               date.strftime('%Y-%m-%d'))
        zz = pd.read_sql(sql, connect)
        zz.dropna(axis = 1,inplace = True)

        ## 计算主体期限结构
        cond = True
        if not term: # 获得主体整条期限结构曲线
            x_observed = temp.loc[:, 'ptmyear']
            y_observed = temp.loc[:, 'yield']
            zz_x_observed = [float(x) for x in zz.columns[2:]]
            zz_y_observed = zz.iloc[0, 2:]
            cond = False
            term = np.linspace(min(x_observed), max(x_observed), num)
        else: # 获取某个期限的加点
            try:
                right = temp.ptmyear[temp.ptmyear >= term].index[0] + 1
                length = temp.shape[0]
                left, right = self.edge_case(right, length)
            except IndexError:
                right = temp.shape[0] - 1
                left = right - 2

            x_observed = temp.loc[left:right,'ptmyear']
            y_observed = temp.loc[left:right,'yield']

            zz_x_observed = [float(x) for x in zz.columns[2:]]
            zz_right = len(zz_x_observed) - len([float(x) for x in zz.columns[2:] if float(x) >= term]) + 1
            if zz_right <= len(zz_x_observed) :
                length = len(zz_x_observed)
                zz_left, zz_right = self.edge_case(zz_right,length)
            else:
                zz_right = len(zz_x_observed) - 1
                zz_left = zz_right - 2
            zz_x_observed = zz_x_observed[zz_left:(zz_right+1)]
            zz_y_observed = zz.iloc[0, (2 + zz_left):(3 + zz_right)]

        y = pchip_interpolate(x_observed, y_observed, term)
        zz_y = pchip_interpolate(zz_x_observed, zz_y_observed, term)

        output = pd.DataFrame([term, y-zz_y])
        output = output.T
        output.columns = ['期限','收益率']
        if not cond: # 当用户想知道整条曲线的期限结构时，绘出加点图
            # plt.plot(x_observed, y_observed, "o", label="{0}观测值".format(issuer))
            # plt.plot(x, y, label="{0}收益率曲线".format(issuer))
            # plt.plot(x,zz_y,label="{0}{1}收益率曲线".format(type,rate))
            # plt.legend()
            # plt.show()

            plt.plot(term, y-zz_y, label="{0}{1}收益率加点曲线".format(type, rate))
            plt.show()
        else:
            print(output)

    def retrieve_data( self,
                       df,
                       metrics,
                       subsets):
        i = 0
        for metric in metrics:
            try:
                df = df[df[metric]== subsets[i]]
            except AttributeError:
                raise ('没找到{0}在{1}中'.format(metric, subsets[i]))
            i += 1

        try:
            return df.B_ANAL_YIELD_CNBD.iloc[0]
        except IndexError:
            return np.nan


    ## 获得关键期限收益率加点历史情况
    def hist_addon_plot(self,
                        term: float,
                        lookbackperiod: int):
        # term：期限 -> 0.08Y
        # nlookbackperiod: 历史数据商都 -> 100(bday)

        issuer = self.issuer
        today = self.date
        columns_for_bondinfo = ['bond_code', 'sec_name', 'redemption_beginning']
        date_range = pd.bdate_range(end = today, periods = lookbackperiod)
        date_tup = tuple(x.strftime('%Y%m%d') for x in date_range)

        ### bondtrade数据库抓取数据bondinfo
        connect = create_engine('mysql+pymysql://houyuhui:UYFnnrAFl9yTbqf3@172.22.213.219:3306/bondtrade')
        sql = 'select bond_code, sec_name, redemption_beginning, maturitydate, issue_firstissue, adj_rate_latestmir_cnbd from bondinfo where sec_name like "__{0}%%";'.format(
            issuer)
        temp = pd.read_sql(sql, connect)
        rate = temp.adj_rate_latestmir_cnbd.iloc[0] # 获取主体评级，后续用function改进
        type = '城投债'  # 获取主体分类，后续用function改进

        ### 抓取收益率数据
        # 选择在这一历史区间内存续过的全部债券
        code_list = temp[(date_range[0] <= temp.redemption_beginning)&(temp.issue_firstissue <= today)].bond_code
        yield_table = self.get_latest_yield(code_list,connect,lookbackperiod)  # 从wind数据库中抓取收益率数据

        # 获取相应评级和种类的中债关键期限收益率曲线
        connect = create_engine('mysql+pymysql://houyuhui:UYFnnrAFl9yTbqf3@172.22.213.219:3306/spreadhedge')
        sql = "select * from {0} where bond_rank='{1}' and trade_date in {2};".format('ytm_' + type + '_1',
                                                                                               rate,
                                                                                               date_tup)
        zz = pd.read_sql(sql, connect)
        zz.dropna(axis = 1, inplace = True)
        zz_x_observed = [float(x) for x in zz.columns[2:]]
        zz_right = len(zz_x_observed) - len([float(x) for x in zz.columns[2:] if float(x) >= term]) + 1
        if zz_right <= len(zz_x_observed):
            length = len(zz_x_observed)
            zz_left, zz_right = self.edge_case(zz_right, length)
        else:
            zz_right = len(zz_x_observed) - 1
            zz_left = zz_right - 2
        zz_x_observed = zz_x_observed[zz_left:(zz_right + 1)]


        ## 循环获取每日hermite插值得到的收益率曲线或关键期限点值
        output = pd.DataFrame(columns = ['收益率'])
        for date in date_range:
            date = date.date()

            ## 数据预处理
            # 选择当天有收益率的数据，即当天存续债券
            temp_yield = yield_table[(yield_table.TRADE_DT == date.strftime('%Y%m%d'))&(yield_table.B_ANAL_CREDIBILITY == '推荐')]
            temp1 = temp[temp.bond_code.isin(temp_yield.S_INFO_WINDCODE)]
            temp1 = temp1[columns_for_bondinfo]
            # 计算存续债券当天距到期的期限
            temp1['ptmyear'] = temp1.redemption_beginning.apply(lambda x: round(float((x - date).days) / 365, 3))
            # 获得对应债券的收益率
            temp1['yield'] = list(temp_yield.iloc[pd.Index(temp_yield.S_INFO_WINDCODE).get_indexer(temp1.bond_code)][
                'B_ANAL_YIELD_CNBD'])
            temp1.dropna(inplace = True)
            temp1.sort_values(by = 'ptmyear', ascending = True, inplace = True)
            temp1.drop_duplicates(['ptmyear'], inplace = True, ignore_index = True)  # 相同期限的债券只取其中一只
            if temp1.empty:
                print("{0}{1}没有存续债券".format(date,issuer))
                continue

            ## 处理当所给期限为边角值的情况
            try:
                right = temp1.ptmyear[temp1.ptmyear >= term].index[0] + 1
                length = temp1.shape[0]
                left, right = self.edge_case(right, length)
            except IndexError:
                right = temp1.shape[0] - 1
                left = right - 2
            x_observed = temp1.loc[left:right, 'ptmyear']
            y_observed = temp1.loc[left:right, 'yield']
            try:
                zz_y_observed = zz[zz.trade_date == date].iloc[0, (2 + zz_left):(3 + zz_right)]
            except IndexError:
                continue

            ## 对本主体及其对应中债隐含评级进行插值得到完整期限曲线
            y = pchip_interpolate(x_observed, y_observed, term)
            zz_y = pchip_interpolate(zz_x_observed, zz_y_observed, term)
            # 得到当天加点情况
            output.loc[date, '收益率'] = y - zz_y

        # self.date = datetime.datetime.now().date() - BDay(1)
        plt.plot(output.index, output['收益率'], label="期限  {0}  {1}  收益率{2}天历史加点曲线".format(term,self.issuer,lookbackperiod))
        plt.show()


        # return output


if __name__ == '__main__':
    ### 参数部分
    Ttotal = time.perf_counter()
    issuer = '津城建'
    date = (datetime.datetime.now().date() - BDay(1)).date()
    # date = (datetime.datetime.now().date() - BDay(300)).date()
    # date = datetime.date(2021,9,27)
    cao = Curve_Addon(issuer,date)
    # output = cao.hist_addon_plot(0.5,300)
    cao.hermite_curve_daily(num = 1000)
    # output = cao.hermite_curve_daily(40)
    # output.to_excel('test_2.xlsx')
    Ttotal1 = time.perf_counter()
    print('总时间{0}'.format(Ttotal1-Ttotal))

    pass