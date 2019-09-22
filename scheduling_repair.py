# coding=utf-8

"""
调度历史修复程序
schedule_name: str/调度名称
schedule_parent_name: str/父调度名称
schedule_time: str/调度执行时间
schedule_dir: str/调度文件目录
schedule_author: str/调度负责人
schedule_switch: int/调度开关 0:关闭 1:打开
is_extra: int/执行对象 0：仅输入的对象 1：输入对象+子调度 2：前一日所有失败任务
state_date: str/调度执行开始日期
end_date: str/调度执行结束日期
"""

import sys
sys.path.append("F:/pycode/mysql_scheduling/config")
import schedule

# 修复调度
# schedule_name = "dwd_evt_recruit_histroy_state_dd"
# schedule_name = "dwd_evt_recruit_histroy_detail_dd"
# schedule_name = "st_simple_report_dd"
schedule_name = "ods_evt_electricity_dd"
# 执行方式
is_extra = 1
# 执行时间
start_date = '20190921'
end_date = '20190921'

print("#####################################")
if __name__ == '__main__':
    schedule.repair(schedule_name, is_extra, start_date, end_date)


