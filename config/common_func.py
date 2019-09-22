# coding=utf-8


import logging
import datetime
import os


def log(filename=''):
    if filename == '':
        pass
    elif os.path.exists(os.path.dirname(filename)):
        pass
    else:
        print(u"文件所在的文件夹路径不存在({0})".format(filename))
        exit(1)

    logger = logging.getLogger()
    logger.handlers = []
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(u'%(asctime)s [%(filename)s:%(lineno)d][%(levelname)s]: %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')

    fh = logging.FileHandler(filename, mode='a')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)


# log('F:\\pycode\\log\\mysql_scheduling\\ods_evt_recruit_data_job_detail_ds\\123.txt')
# logging.info('123')


def exec_date_list(start_date, end_date):
    """
    执行时间函数
    :param start_date: str/%Y%m%d 开始时间
    :param end_date: str/%Y%m%d 结束时间
    :return: date_list: list/['%Y%m%d', '%Y%m%d'...]
    """
    date_list = []
    try:
        start_date = datetime.datetime.strptime(start_date, '%Y%m%d')
        end_date = datetime.datetime.strptime(end_date, '%Y%m%d')
        if start_date > end_date:
            tmp_date = end_date
            end_date = start_date
            start_date = tmp_date
        else:
            pass
        for i in range(0, (end_date-start_date).days+1):
            date_list.append(datetime.datetime.strftime(start_date+datetime.timedelta(days=i), '%Y%m%d'))
        return date_list
    except Exception as e:
        print(Exception, e)
        print(u"start_date:{0},end_date:{1}".format(start_date, end_date))
        exit(1)


def table_convert(data, row_key, col_key, data_key, is_row_head=True, custom_col=None):
    """
    list[dict]进行行列转换
    :param data：数据源
    :param row_key: 行名对应数据源中的key
    :param col_key: 列名对应数据源中的key
    :param data_key: 数据对应数据源中的[key]/list
    :param is_row_head: 是否返回行头
    :param custom_col: 新增自定义列
    :return result_table: list[list]
    """

    d_key = data[0].keys()
    for i in [row_key, col_key] + data_key:
        if i in d_key:
            pass
        else:
            print(i, " not in data's key")
            exit(1)
    col_name = list(set(data_var[col_key] for data_var in data))
    col_name.insert(0, row_key)
    col_len = len(col_name)
    row_name = list(set(data_var[row_key] for data_var in data))
    row_name.sort()
    row_len = len(row_name)

    result_table = []
    for rep_num in range(row_len):
        result_table.append(range(col_len))
        result_table[rep_num][0] = row_name[rep_num]

    for data_var in data:
        row_num = row_name.index(data_var[row_key])
        col_num = col_name.index(data_var[col_key])
        if len(data_key) > 1:
            result_table[row_num][col_num] = '/'.join([str(data_var[data_key_var]) for data_key_var in data_key])
        else:
            result_table[row_num][col_num] = data_var[data_key[0]]

    if custom_col is not None:
        for result_table_var in result_table:
            result_table_var.insert(0, str(custom_col))
        col_name.insert(0, '统计项')
    if is_row_head is True:
        result_table.insert(0, col_name)

    return result_table


def map_str(var):
    """
    将对象var里的所有元素转成str
    :param var: 执行对象 list/tuple/str/int/dict
    :return var: 元素字符化的var
    """
    if type(var) is list or type(var) is tuple:
        var = map(map_str, var)
    elif type(var) is dict:
        var = map(map_str, var.values())
    else:
        var = str(var)
    return var
