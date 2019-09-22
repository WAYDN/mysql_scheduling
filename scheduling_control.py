# coding=utf-8

"""
调度正常执行程序
schedule_name: str/调度名称
schedule_parent_name: str/父调度名称
schedule_time: str/调度执行时间
schedule_dir: str/调度文件目录
schedule_author: str/调度负责人
schedule_switch: int/调度开关 0:关闭 1:打开
"""
import sys
import datetime
import time
import os
sys.path.append("F://pycode//mysql_scheduling//config")
import schedule


schedule_list = schedule.mysql_to_exec_list()
# print [schedule_var for schedule_var in schedule_list]
# exit()

print("################################################################")
now = datetime.datetime.now()
if __name__ == '__main__':
    print(u"开始执行调度程序：{0}".format(now))
    try:
        schedule.iterator(schedule_list, 1)
        # print(schedule_list)
    except Exception as e:
        print(e)
        os.exit(1)
    print(u"调度结束")
    time.sleep(10)







