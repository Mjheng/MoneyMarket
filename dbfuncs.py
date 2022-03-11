# -*- coding: utf-8 -*-
import datetime,pymysql,cx_Oracle
import pandas as pd
from sqlalchemy import create_engine


DBConfs = {
    "BondTrade": {
        'host': '172.22.213.219',
        'user': 'houyuhui',
        'password': 'YFnnrAFl9yTbqf3',
        'db': 'bondtrade',
        'port': 3306
    },
    "Wind": {
        'host': '10.23.153.15',
        'user': 'li_hai',
        'password': 'li_hai',
        'db': '',
        'sid': 'wind',
        'service_name': '',
        'port': 21010
    },
}

# DataFrame数据写入数据库;index:索引是否作为一列写入数据库;index_name:为索引重命名;table_index:数据表主键;table_columns:要写入的列，默认为全写入
def to_sql(df,table_name,index=False,index_name='',table_index=[],table_columns=[],auto_int_index=False,chunksize=5000,DBConf="Project"):
    # 数据库配置
    DBConf = DBConfs[DBConf]

    # df数据检查
    if 0 == df.shape[0]:
        return "Empty Data."
    # df索引名称检查
    if True == index:
        if None == df.index.name:
            if '' == index_name:
                return "Index Name Is None And Not Appointed."
            else:
                df.index.name = index_name
        # 将索引变为一列
        df.reset_index(inplace=True)

    # 数据表主键、非主键检查
    table_index = set(table_index)
    table_columns = set(table_columns)

    # 主键需在DataFrame中存在
    if len(table_index) > 0:
        if len(table_index - set(df.columns)) > 0:
            return "Factor(table_index) Error."

    # 非主键需在DataFrame中存在
    if len(table_columns) > 0:
        if len(table_columns - set(df.columns)) > 0:
            return "Factor(columns) Error."
        table_columns = table_columns - table_index
    else:
        table_columns = set(df.columns) - table_index

    # 所有需写入数据库的数据列
    columns = table_index | table_columns

    # 开始进行数据写入
    cnn = pymysql.connect(host=DBConf['host'], user=DBConf['user'], password=DBConf['password'], db=DBConf['db'], port=DBConf['port'], charset='utf8', use_unicode=False)  # 指定链接格式为UTF8
    cur = cnn.cursor()

    # 分批次提交
    chunkcount = 0
    for i, row in df.iterrows():
        try:
            row.fillna('Null', inplace=True)
            #row.loc[row == 'nan'] = 'Null'
            row = row.apply(lambda x: 'Null' if 'nan' == x else x)
            # 生成sql关键数据片段
            cls_str = ''
            val_str = ''
            val_val = []
            update_str = ''
            for x in columns:
                cls_str += "`" + x + "`,"
                val_str += "'%s',"
                val_val.append(pymysql.escape_string(row[x]) if str == type(row[x]) else row[x])
            if False == auto_int_index:
                for x in table_columns:
                    update_str += "`" + x + "`='%s',"
                    val_val.append(pymysql.escape_string(row[x]) if str == type(row[x]) else row[x])
            # 拼接生成sql语句
            if False == auto_int_index:
                sql = "INSERT INTO `" + table_name + "` (" + cls_str[:-1] + ") VALUES (" + val_str[:-1] + ") ON DUPLICATE KEY UPDATE " + update_str[:-1]
            else:
                sql = "INSERT INTO `" + table_name + "` (" + cls_str[:-1] + ") VALUES (" + val_str[:-1] + ")"
            sql = sql.replace("%s", "--s")
            sql = sql.replace("%", "&&")
            sql = sql.replace("--s", "%s")
            sql = sql % tuple(val_val)
            sql = sql.replace("&&", "%")
            sql = sql.replace("'Null'", "Null")
            cur.execute(sql)
            chunkcount += 1
        except Exception as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))
        if chunkcount >= chunksize:
            chunkcount = 0
            try:
                cnn.commit()  # 事务提交
            except Exception as e:
                print("Error %d: %s" % (e.args[0], e.args[1]))
    try:
        cnn.commit()  # 事务提交
    except Exception as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    cur.close()
    cnn.close()

# 从数据库中读取数据;table_name:数据表名;table_columns:读取的列,默认为全部;index:索引是否作为一列写入数据库;index_name:为索引重命名
def from_sql(table_name,table_columns=[],columns_map={},where='',order='',limit='',index=False,index_name='',DBConf="Project"):
    # 数据库配置
    DBConf = DBConfs[DBConf]
    # 列名映射检查
    if len(columns_map) > 0 and len(table_columns) > 0:
        if len(set(columns_map.keys()) - set(table_columns)) > 0:
            return "Columns Map Has More Column Than Table Columns."
    # 所有需要返回的列
    columns = (set(columns_map.values()) | set(table_columns) - set(columns_map.keys())) if len(columns_map) > 0 else set(table_columns)
    # index检查
    if True == index:
        if index_name not in columns:
            return "Index Name Is Not In Table Index List."
    # 生成sql关键数据片段
    cls_str = ''
    if 0 == len(table_columns):
        cls_str += "* "
    else:
        for x in table_columns:
            cls_str += "`" + x + "`,"
    # 生成sql
    sql = "SELECT " + cls_str[:-1] + " FROM `" + table_name + "` "
    if '' != where:
        sql += "WHERE " + where + " "
    if '' != order:
        sql += "ORDER BY " + order + " "
    if '' != limit:
        sql += "LIMIT " + limit + " "
    # 获取数据
    con = pymysql.connect(host=DBConf['host'], user=DBConf['user'], password=DBConf['password'], db=DBConf['db'], port=DBConf['port'], charset='utf8', use_unicode=True)  # 指定链接格式为UTF8
    df = pd.read_sql(sql, con)
    con.close()
    # 列名映射
    df.rename(columns=columns_map, inplace=True)
    # 重设索引
    if True == index:
        df.set_index(index_name,inplace=True)
    return df

# 从数据库中读取数据;执行自定义sql语句;table_columns:读取的列,默认为全部;index:返回DataFrame数据是否设置索引;index_name:为索引重命名
def from_sql_manually(sql,columns_map={},index=False,index_name='',DBConf="Project"):
    # 数据库配置
    DBConf = DBConfs[DBConf]
    # 获取数据
    con = pymysql.connect(host=DBConf['host'], user=DBConf['user'], password=DBConf['password'], db=DBConf['db'],
                          port=DBConf['port'], charset='utf8', use_unicode=True)  # 指定链接格式为UTF8
    df = pd.read_sql(sql, con)
    con.close()
    # 列名映射
    if len(columns_map) > 0:
        df.rename(columns=columns_map, inplace=True)
    # 重设索引
    if True == index:
        df.set_index(index_name, inplace=True)
    return df

# 读取数据(Oracle数据库)
def from_sql_orcl(table_name,table_columns=[],columns_map={},where='',order='',index=False,index_name='',DBConf="Wind"):
    # 数据库配置
    DBConf = DBConfs[DBConf]
    # 生成sql关键数据片段
    cls_str = ''
    if 0 == len(table_columns):
        cls_str += "* "
    else:
        cls_str = ','.join(table_columns)
    # 生成sql
    sql = "SELECT " + cls_str + " FROM " + table_name + " "
    if '' != where:
        sql += "WHERE " + where + " "
    if '' != order:
        sql += "ORDER BY " + order + " "
    # 获取数据
    # print(sql)
    sid = DBConf.get('sid', '')
    service_name = DBConf.get('service_name', '')
    if '' != service_name:
        tns = cx_Oracle.makedsn(DBConf['host'], DBConf['port'], service_name = service_name)
    else:
        tns = cx_Oracle.makedsn(DBConf['host'], DBConf['port'], sid)
    con = cx_Oracle.connect(DBConf['user'], DBConf['password'], tns)
    # cur = con.cursor()
    # cur.execute(sql)
    df = pd.read_sql(sql, con)
    con.close()
    # 列名映射
    df.rename(columns=columns_map, inplace=True)
    # 重设索引
    if True == index:
        df.set_index(index_name,inplace=True)
    return df

# 从数据库中读取数据;执行自定义sql语句;table_columns:读取的列,默认为全部;index:返回DataFrame数据是否设置索引;index_name:为索引重命名
def from_sql_orcl_manually(sql,columns_map={},index=False,index_name='',DBConf="Wind"):
    # 数据库配置
    DBConf = DBConfs[DBConf]
    # 获取数据
    # print(sql)
    sid = DBConf.get('sid', '')
    service_name = DBConf.get('service_name', '')
    if '' != service_name:
        tns = cx_Oracle.makedsn(DBConf['host'], DBConf['port'], service_name = service_name)
    else:
        tns = cx_Oracle.makedsn(DBConf['host'], DBConf['port'], sid)
    con = cx_Oracle.connect(DBConf['user'], DBConf['password'], tns)
    df = pd.read_sql(sql, con)
    con.close()
    # 列名映射
    if len(columns_map) > 0:
        df.rename(columns=columns_map, inplace=True)
    # 重设索引
    if True == index:
        df.set_index(index_name, inplace=True)
    return df
