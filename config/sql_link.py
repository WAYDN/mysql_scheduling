#coding=utf-8

"""
1.连接mysql，并执行sql，返回执行的结果集
"""


import MySQLdb
import logging
import re


def mysql(sql, is_print=1, is_return=0, return_type=None):
    """
    :param sql: string/需执行的sql
    :param is_print: int/是否打印sql，默认打印
    :param is_return: int/是否返回结果集
    :param return_type: int/结果返回类型，默认list_dict,否则list_tuple
    :return: list/返回执行的结果集
    """
    if is_print == 1:
        logging.info(u"执行sql:" + sql)
    # sql = re.sub(r'\n', ' ', sql)
    db = MySQLdb.connect(host="127.0.0.1", port=3306, user="waydn", passwd="gauss412", db="test", charset='utf8')
    if return_type is None:
        cur = db.cursor(MySQLdb.cursors.DictCursor)
    else:
        cur = db.cursor()
    try:
        cur.execute(sql)
    except MySQLdb.Error as e:
        logging.error("执行错误:{0}".format(str(e)))
        exit(1)
    # 向mysql提交事务
    db.commit()
    result = cur.fetchall()
    cur.close()
    db.close()
    if is_return == 1:
        return list(result)


def mysql_insert_sql(db, k_v, data, *var):
    """
    将data的内容
    :param db: string/插入的表名
    :param k_v: dict/字段名对应data的key
    :param data: list[dict]/需要写入的数据，list中嵌套dict{字段：值},新增字段对应的值为''
    :param *var: list/补充常量数据
    :return: 返回sql语句
    """
    data_num = len(data)
    keys = k_v.keys()
    sql = "insert into {0} (".format(db) + ",".join(keys) + ") values "
    for i in range(data_num):
        # keys 为表字段，这里的j要用data的字段
        tmp_value = []
        h = 0
        for j in keys:
            if k_v[j] == "":
                # 补充源数据没有的字段，例如p_date
                tmp_value.append(str(var[h]))
                h += 1
            elif type(data[i][k_v[j]]) is int:
                tmp_value.append(str(data[i][k_v[j]]))
            elif type(data[i][k_v[j]]) is list:
                tmp_value.append("'" + ','.join(data[i][k_v[j]]) + "'")
            else:
                tmp_value.append("'" + re.sub(r"'", "\"", str(data[i][k_v[j]])) + "'")
        # print(tmp_value)
        # print(",".join(tmp_value))
        # exit()
        sql = sql + "(" + ",".join(tmp_value) + ")"
        if i < data_num - 1:
            sql += ","
        else:
            sql += ";"
    return sql

# import datetime
# exec_date = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
# a = logging.getLogger("")
# a.setLevel(logging.INFO)

# mysql("""insert into sys.ods_evt_schedule_detail_dd (schedule_time,schedule_name,state_code,schedule_author,p_date) values ('03:57:08','ods_evt_recruit_data_job_detail_ds',1,'W',20180207);""")
# print mysql("""select * from db.dwd_evt_recruit_histroy_state_dd limit 10;""",1,1)

# print(mysql_insert_sql('test.tmp1', {'schedule_time': 'schedule_time', 'schedule_name': 'schedule_name',
#                                      'schedule_author': 'schedule_author', 'state_code': 'state_code', 'p_date': ''},
#                        [{'schedule_dir': 'F:/pycode/mysql_scheduling', 'schedule_time': '21:17:05',
#                          'schedule_name': 'dwd_evt_recruit_histroy_state_dd', 'schedule_author': '',
#                          'schedule_switch': 1, 'schedule_parent_name': ['ods_evt_recruit_data_job_detail_ds'],
#                          'state_code': 1}], '20171212'))

