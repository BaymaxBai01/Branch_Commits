import datetime
import time
import os

# 使用局部变量方法导入，控制资源部分导入
# __all__ = ['path_file_1','path_file_2','path_file_test','start_time','end_time']

def GetDesktopPath():
    """获取本地电脑名称路径"""
    return os.path.join(os.path.expanduser("~"))

Total_path = GetDesktopPath().split('\\')[-1] # 获取电脑名称
start_time = datetime.date(datetime.date.today().year, datetime.date.today().month, 1)
end_time = datetime.date(datetime.date.today().year, datetime.date.today().month + 1, 1) - datetime.timedelta(1)
StartTime = str(start_time)
EndTime = str(end_time)
path_file_1 = r"C:\Users\{}\OneDrive - Microsoft\办公文件\tool\Python_Get_Github_api\data_source\data logs\{}_URL_STATUS_CODE_update_at_{}.csv".format(Total_path,StartTime,time.strftime('%Y-%m-%d %H.%M.%S',time.localtime(time.time())))
path_file_2 = r"C:\Users\{}\OneDrive - Microsoft\办公文件\tool\Python_Get_Github_api\data_source\data logs\{}_URL_DATA_LOGS_update_at_{}.csv".format(Total_path,StartTime,time.strftime('%Y-%m-%d %H.%M.%S',time.localtime(time.time())))
path_file_3 = r"C:\Users\{}\OneDrive - Microsoft\办公文件\tool\Python_Get_Github_api\data_source\data logs\{}_BRANCH_COMMITS_DATA_update_at_{}.csv".format(Total_path,StartTime,time.strftime('%Y-%m-%d',time.localtime(time.time())))
path_file_4 = r"C:\Users\{}\OneDrive - Microsoft\办公文件\tool\Python_Get_Github_api\data_source\data logs\{}_TOTAL_REPO_URL_update_at_{}.csv".format(Total_path,StartTime,time.strftime('%Y-%m-%d %H.%M.%S',time.localtime(time.time())))

# 过滤执行代码
if __name__ == '__main__':
    # 可以在这里书写执行代码/测试代码/说明文字，被导入时不会被执行
    print(path_file_1)
    print(path_file_2)
    print(path_file_3)
    print(path_file_4)
    print([start_time,end_time])