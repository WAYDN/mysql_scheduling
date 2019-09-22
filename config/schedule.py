# coding=utf-8

import os
import re
import datetime
import sys
import logging
import multiprocessing
import Email
import common_func
import sql_link
import time


def mysql_to_exec_list(schedule_name='', is_extra=0):
    """
    获取待执行的调度信息
    :param schedule_name: str/待执行调度名；
    :param is_extra: int/ 1:获取其子级任务,2:获取前一日所有失败任务
    :return: list/执行调度列表
    """
    now = (datetime.datetime.now()).strftime('%Y%m%d')
    last_1_day = (datetime.datetime.strptime(now, '%Y%m%d') + datetime.timedelta(days=-1)).strftime('%Y%m%d')

    exce_sql = "select * from sys.dim_evt_schedule_detail"
    if schedule_name != '':
        exce_sql = exce_sql + " where schedule_name = '{0}'".format(schedule_name)
    schedule_list = sql_link.mysql(sql=exce_sql, is_print=0, is_return=1)

    if len(schedule_list) == 0:
        print(u'schedule_name:{0} 不存在'.format(schedule_name))
        exit(1)

    if is_extra == 1:
        sublevel_schedule_list = sql_link.mysql(sql="select * from sys.dim_evt_schedule_detail "
                                                    "where schedule_parent_name rlike '{0}'".
                                                format(schedule_list[0]['schedule_name']), is_print=0, is_return=1)
        while len(sublevel_schedule_list) != 0:
            schedule_list_value = [schedule_list_var["schedule_name"] for schedule_list_var in schedule_list]
            for sublevel_schedule_list_var in sublevel_schedule_list:
                if sublevel_schedule_list_var["schedule_name"] not in schedule_list_value:
                    schedule_list.append(sublevel_schedule_list_var)
            tmp_result = []
            for tmp_var in sublevel_schedule_list:
                tmp_result += sql_link.mysql(sql="select * from sys.dim_evt_schedule_detail "
                                                 "where schedule_parent_name rlike '{0}'".
                                             format(tmp_var['schedule_name']), is_print=1, is_return=1)
            sublevel_schedule_list = tmp_result
    elif is_extra == 2:
        sublevel_schedule_list = sql_link.mysql(sql="select * from sys.ods_evt_schedule_detail_dd "
                                                    "where state_code = -1 and p_date={0}".
                                                format(last_1_day), is_print=0, is_return=1)
        schedule_list += sublevel_schedule_list

    for tmp_dict in schedule_list:
        if tmp_dict['schedule_parent_name'] is None:
            tmp_dict['schedule_parent_name'] = []
        else:
            tmp_dict['schedule_parent_name'] = tmp_dict['schedule_parent_name'].split(',')
    return schedule_list


def state(schedule_name, schedule_var, schedule_code, info):
    """
    将状态码写入到调度信息，并返回信息
    :param schedule_name: str/调度名称
    :param schedule_var: dict/调度信息
    :param schedule_code: int/调度状态码
    :param info: str/需打印的信息
    :return: dict/调度信息
    """
    schedule_var['state_code'] = schedule_code
    if schedule_code == 0:
        logging.warning('{0}:{1}'.format(schedule_name, info))
    elif schedule_code == -1:
        logging.error('{0}:{1}'.format(schedule_name, info))
    elif schedule_code == 1:
        logging.info('{0}:{1}'.format(schedule_name, info))
    return schedule_var


def exec_file(schedule_var, exec_date=''):
    """
    执行文件，并将文件名定义为变量，并赋予值1(表示执行成功,执行失败则返回0),并输出日志
    额外的状态码 -1：父任务关闭 -2：父任务关闭
    :param schedule_var: dict/调度信息，(schedule_name: string/执行的文件名,
                                        schedule_dir: string/执行文件的目录,
                                        author_email: string/执行文件的负责人邮箱,
                                        schedule_switch: int/是否执行文件 0/1,
                                        schedule_parent_name: list/父任务列表)
    :param exec_date: str/调度执行的日期变量，'20170806'
    :return:
    """
    # 默认执行的时间为前一天
    if exec_date == '':
        exec_date = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')

    filename = schedule_var['schedule_name']
    dir_name = schedule_var['schedule_dir']
    switch = schedule_var['schedule_switch']
    if 'author_email' not in schedule_var.keys() or re.match(r'.*@.*', schedule_var['author_email']) is None:
        author_email = 'ernestw4q12@163.com'
    else:
        author_email = schedule_var['author_email']
    schedule_parent_name = "[{0}]".format(','.join(schedule_var['schedule_parent_name']))

    s_time = datetime.datetime.now()
    now = s_time.strftime("%Y%m%d")
    # 日志写入到日志文件log_file
    log_dir = 'F:/pycode/mysql_scheduling/log/{0}/{1}'.format(dir_name.split("/")[-1], filename)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = "{0}/{1}.log".format(log_dir, now)

    common_func.log(log_file)

    if switch == 0:
        state_code = 0
        state_info = '调度已关闭'
        schedule_info = state(filename, schedule_var, state_code, state_info)
    elif switch == 1:
        if 'state_code' not in schedule_var.keys() or schedule_var['state_code'] == 1:
            try:
                sys.path.append(dir_name + "/" + filename)
                # 执行调度文件##############################################################################
                with open(dir_name + "/" + filename + "/" + filename + ".py", 'rb') as f:
                    exec(f.read())
                # logging.error(dir_name + "/" + filename + "/" + filename + ".py")
                state_code = 1
                state_info = '调度执行成功'
                schedule_info = state(filename, schedule_var, state_code, state_info)
            except Exception as e:
                state_code = -1
                state_info = repr(e)
                schedule_info = state(filename, schedule_var, state_code, state_info)
        elif schedule_var['state_code'] == -3:
            state_code = 0
            state_info = '父调度已关闭 父调度：{0}'.format(schedule_parent_name)
            schedule_info = state(filename, schedule_var, state_code, state_info)
        elif schedule_var['state_code'] == -2:
            state_code = -1
            state_info = '父调度执行失败 \n父调度：{0}'.format(schedule_parent_name)
            schedule_info = state(filename, schedule_var, state_code, state_info)

    e_time = datetime.datetime.now()
    if schedule_var['state_code'] == -1:
        subject = '执行报错邮件'
        content = "{0}于{1}执行错误({2}) \n错误原因：{3}".format(filename, datetime.datetime.
                                                       strftime(e_time, '%Y%m%d %H:%M:%S'), exec_date, state_info)
        attachment_file = log_file
        email = Email.Email(receivers=author_email, mail_subject=subject, mail_content=content,
                            attachment_file=attachment_file)
        email.send()
        # Email.Email.send(author_email, subject, content, attachment_file)

    # 删除原先调度，再将执行结果返回到mysql
    schedule_var['schedule_time'] = (datetime.datetime.now()).strftime('%H:%M:%S')
    logging.info("{0}:删除原先调度，再将执行结果返回到mysql\n".format(schedule_info['schedule_name']))
    sql_link.mysql(sql="delete from sys.ods_evt_schedule_detail_dd where schedule_name='{0}' and p_date={1}".
                   format(schedule_info['schedule_name'], exec_date), is_print=1)
    k_v = {'schedule_time': 'schedule_time', 'schedule_name': 'schedule_name', 'author_email': 'author_email',
           'state_code': 'state_code', 'p_date': ''}
    insert_sql = sql_link.mysql_insert_sql('sys.ods_evt_schedule_detail_dd', k_v, [schedule_info], exec_date)
    sql_link.mysql(insert_sql, 1)
    sql_result = sql_link.mysql(sql="select * from sys.ods_evt_schedule_detail_dd where schedule_name='{0}' "
                                    "and p_date={1}".
                                format(schedule_info['schedule_name'], exec_date), is_print=1, is_return=1)
    if len(sql_result) == 0:
        logging.warning(u"{0}写入到mysql失败".format(schedule_info['schedule_name']))
    logging.info("{0}-{1} 耗时：{2}\n".format(filename, exec_date, e_time-s_time))

# schedule_list = []
# schedule_list.append({'schedule_name': u'ods_evt_recruit_data_job_detail_ds', 'schedule_parent_name': [], 'schedule_dir': u'F:/pycode/mysql_scheduling', 'schedule_author': u'ernestw4q12@163.com', 'schedule_switch': 1})
# # schedule_list.append({'schedule_name': 'dwd_evt_recruit_histroy_state_dd', 'schedule_parent_name': ['ods_evt_recruit_data_job_detail_ds'], 'schedule_time': '02:00:00', 'schedule_dir': 'F:/pycode/mysql_scheduling', 'schedule_author': '', 'schedule_switch': 1})
# exec_file(schedule_list[0], '20171215')


def iterator(schedule_list, is_all=0, exec_date=''):
    """
    执行调度列表，将列表分成待执行列表和执行列表，多进程执行调度列表，在将待执行列表传值给调度列表，直至执行完毕
    :param schedule_list: list/调度列表
    :param exec_date: str/调度执行时间(yyyyMMdd)
    :param is_all: int/是否为全调度执行或回滚
    :return:
    """
    now = datetime.datetime.now()
    now_date = now.strftime("%Y%m%d")
    # 最大执行进程数
    process_max_num = 2
    # 默认执行的日期变量为前一天
    if exec_date == '':
        exec_date = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    if is_all == 1:
        sql_link.mysql(sql="delete from sys.ods_evt_schedule_detail_dd where p_date={0}".format(exec_date), is_print=1)

    common_func.log('F:/pycode/mysql_scheduling/log/exec_log/{0}.log'.format(now_date))

    while len(schedule_list) > 0:
        schedule_num = len(schedule_list)
        exec_list = []
        tmp_schedule_list = []
        for n in range(schedule_num):
            schedule_var = schedule_list[n]
            # 状态码schedule_switch 异常的均将开关打成0，作为关闭处理，并执行
            if schedule_var['schedule_switch'] != 1 or 'schedule_switch' not in schedule_var.keys():
                schedule_list[n]['schedule_switch'] = 0
                exec_list.append(schedule_var)
            else:
                if len(schedule_var['schedule_parent_name']) == 0:
                    exec_list.append(schedule_var)
                else:
                    parent_schedule_num = len(schedule_var['schedule_parent_name'])
                    for parent_schedule_var in schedule_var['schedule_parent_name']:
                        parent_state_code = sql_link.mysql(sql="select a.state_code from "
                                                               "sys.ods_evt_schedule_detail_dd a where a.p_date={0} "
                                                               "and a.schedule_name='{1}'".format(exec_date,
                                                                                                  parent_schedule_var),
                                                           is_print=0, is_return=1)
                        # print int(parent_state_code[0]['state_code'])==1
                        # parent_state_code = [{'state_code': 1}]
                        # 历史修复开后门
                        if len(sys.argv) == 6 and sys.argv[5] == "ERNEST":
                            parent_state_code = [{'state_code': 1}]
                        else:
                            pass

                        if len(parent_state_code) == 0:
                            tmp_schedule_list.append(schedule_var)
                            break
                        elif parent_state_code[0]['state_code'] == 1:
                            parent_schedule_num -= 1
                            if parent_schedule_num == 0:
                                exec_list.append(schedule_var)
                            else:
                                continue
                        elif parent_state_code[0]['state_code'] == 0:
                            schedule_list[n]['state_code'] = -3
                            exec_list.append(schedule_var)
                        elif parent_state_code[0]['state_code'] == -1:
                            schedule_list[n]['state_code'] = -2
                            exec_list.append(schedule_var)
                        else:
                            logging.critical('出现异常状态码：{0} \n暂停所有调度'.format(parent_state_code[0]['state_code']))
                            exit(-1)

        # 将待调度任务赋给执行列表中
        schedule_list = tmp_schedule_list
        # logging.error(schedule_list)

        # 多进程执行exec_list
        if len(exec_list) != 0:
            exec_process = multiprocessing.Pool(process_max_num)
            # print exec_list
            for exec_args in exec_list:
                exec_process.apply_async(func=exec_file, args=(exec_args, exec_date))
            exec_process.close()
            exec_process.join()
        else:
            logging.warning(u'当前未存在任何可执行调度')
            break
    common_func.log('F:/pycode/mysql_scheduling/log/exec_log/{0}.log'.format(now_date))
    logging.info("当前调度列表为空")


def repair(schedule_name, is_extra, start_date, end_date):
    """
    调度历史修复系统
    :param schedule_name: str/调度名称
    :param is_extra: int/执行对象 0：仅输入的对象 1：输入对象+子调度 2：前一日所有失败任务
    :param state_date: str/调度执行开始日期
    :param end_date: str/调度执行结束日期
    :return:
    """
    common_func.log('F:/pycode/mysql_scheduling/log/repair_log/{0}-{1}.log'.format(start_date, end_date))

    schedule_list = mysql_to_exec_list(schedule_name=schedule_name, is_extra=is_extra)
    date_list = common_func.exec_date_list(start_date, end_date)
    date_cnt = len(date_list)
    date_number = 0
    now = datetime.datetime.now()

    logging.info(u"开始执行调度修复程序：{0}".format(now))
    try:
        for exec_date in date_list:
            print(exec_date)
            date_number += 1
            date_finish_rate = round(date_number / date_cnt) * 100
            logging.info(u"调度{0}修复日期{1}-完成度{2}%".format(schedule_name, exec_date, date_finish_rate))
            while len(sys.argv) <= 1:
                sys.argv.append('')
            sys.argv[1] = exec_date
            iterator(schedule_list, 1, sys.argv[1])
            time.sleep(3)
    except Exception as e:
        print(e)
        exit(1)
    logging.info(u"调度历史修复调度结束")
    time.sleep(10)



# repair("dwd_evt_recruit_histroy_state_dd", 0, '20180208', '20180208')
# print common_func.exec_date_list('20180102', '20180103')


#
# schedule_list = []
# schedule_list.append({'schedule_name': 'ods_evt_recruit_data_job_detail_ds', 'schedule_parent_name': [], 'schedule_time': '02:00:00', 'schedule_dir': 'F:/pycode/mysql_scheduling', 'schedule_author': '', 'schedule_switch': 1})
# # schedule_list.append({'schedule_name': 'ods_evt_recruit_data_job_detail_d', 'schedule_parent_name': ['ods_evt_recruit_data_job_detail_ds'], 'schedule_time': '02:30:00', 'schedule_dir': 'F:/pycode/mysql_scheduling', 'schedule_author': '', 'schedule_switch': 1})
# if __name__ == '__main__':
#     schedule_list = mysql_to_exec_list('ods_evt_recruit_data_job_detail_ds', 1)
#     print [schedule_list_var["schedule_name"] for schedule_list_var in schedule_list]
#     iterator(schedule_list)
