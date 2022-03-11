from sqlalchemy import create_engine
import pymysql
import MySQLdb
import pandas as pd
import time
import datetime
from pandas.tseries.offsets import BDay
import dbfuncs

today = (datetime.datetime.now().date() - BDay(1)).date().strftime('%Y%m%d')
lookbackperiod = 300
startdate = ((datetime.datetime.now().date() - BDay(1)).date() - BDay(lookbackperiod)).strftime('%Y%m%d')

start = time.time()
issuer = '津城建'

connect = create_engine('mysql+pymysql://houyuhui:UYFnnrAFl9yTbqf3@172.22.213.219:3306/bondtrade')
sql_1 = 'select bond_code, sec_name, redemption_beginning, maturitydate, issue_firstissue, adj_rate_latestmir_cnbd from bondinfo where sec_name like "__{0}%%";'.format(
            issuer)
temp_1 = pd.read_sql(sql_1, connect)
code_list = temp_1.bond_code
middle = time.time()
print(middle-start)
# sql_2 = 'select s_info_windcode,trade_dt,b_anal_yield_cnbd from cbondanalysiscnbd_his where s_info_windcode in {0} and trade_dt between "{1}" and "{2}"'.format(tuple(code_list),startdate,today)
# sql_2 = 'select s_info_windcode,trade_dt,b_anal_yield_cnbd from cbondanalysiscnbd_his where s_info_windcode in {0} and trade_dt="{1}"'.format(tuple(code_list),startdate)
# sql_2 = 'select t.s_info_windcode,t.trade_dt,t.b_anal_yield_cnbd from wind.cbondanalysiscnbd t where t.s_info_windcode in {0} and t.trade_dt between "{1}" and "{2}"'.format(tuple(code_list),startdate,today)
sql_2 = "select s_info_windcode,trade_dt,b_anal_yield_cnbd from wind.cbondanalysiscnbd t where s_info_windcode in {0} and trade_dt between '{1}' and '{2}'".format(tuple(code_list),startdate,today)
res = dbfuncs.from_sql_orcl_manually(sql_2, DBConf="Wind")
# test = res[res.TRADE_DT > '20220101']
# temp_2 = pd.read_sql(sql_2, connect)
end = time.time()
print(end-start)
pass