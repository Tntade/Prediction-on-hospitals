import multiprocessing
import functools
import datetime
import re
import calendar


def to_ymd(date_str):
    date_str = re.sub(r'\s', '', str(date_str))
    if '-' in date_str or '/' in date_str:
        ymd = re.split('-|/', date_str)
        if len(ymd) >= 3:
            y = int(ymd[0])
            m = int(ymd[1])
            d = int(ymd[2])
        else:
            y = int(ymd[0])
            m = int(ymd[1])
            d = None
    elif date_str.isdigit() and len(date_str) in (6, 8):
        if len(date_str) == 8:
            y = int(date_str[:4])
            m = int(date_str[4:6])
            d = int(date_str[6:])
        else:
            y = int(date_str[:4])
            m = int(date_str[4:6])
            d = None
    else:
        raise Exception("日期格式必须为“YYYY-MM-DD”或“YYYYMMDD”或“YYYY-MM”")
    assert y > 0 and 1 <= m <= 12 and (
        d is None or 1 <= d <= calendar.monthrange(y, m)[1]), "日期错误！"
    return y, m, d


def get_time_windows2(start_date, end_date, window_size, step_size):
    start_date = str(start_date).replace(' ', '')
    end_date = str(end_date).replace(' ', '')
    # 转ymd
    start_y, start_m, start_d = to_ymd(start_date)
    end_y, end_m, end_d = to_ymd(end_date) 
    # 若开始日期的day为空，则置于当月1日
    if start_d is None:
        start_d = 1
    # 若结束日期的day为空，则置于当月最后一天
    if end_d is None:
        end_d = calendar.monthrange(end_y, end_m)[1]
    
    time_windows = []
    y1, m1, d1 = start_y, start_m, start_d
    ymd1 = datetime.date(y1, m1, d1)
    flag = True
    while flag:
        y2 = y1
        m2 = m1 + window_size - 1
        if m2 > 12:
            m2 -= 12
            y2 += 1
        d2 = calendar.monthrange(y2, m2)[1]
        ymd2 = datetime.date(y2, m2, d2)
        
        if ymd2 > datetime.date(end_y, end_m, end_d):
            ymd2 = datetime.date(end_y, end_m, end_d)
            flag = False
        time_windows.append([str(ymd1), str(ymd2)])
        
        m1 += step_size
        if m1 > 12:
            m1 -= 12
            y1 += 1
        d1 = 1
        ymd1 = datetime.date(y1, m1, d1)
        if ymd1 >= datetime.date(end_y, end_m, end_d):
            break    
    
    return time_windows


def with_timeout(timeout):
    """
    timeout装饰器
    """
    def decorator(decorated):
        @functools.wraps(decorated)
        def inner(*args, **kwargs):
            pool = multiprocessing.pool.ThreadPool(1)
            async_result = pool.apply_async(decorated, args, kwargs)
            try:
                return async_result.get(timeout)
            except multiprocessing.TimeoutError:
                return None
            except Exception:
                return None
        return inner
    return decorator


# 重构multiprocessing.Process类，将进程始终定义为非守护进程
class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


# # 重构multiprocessing.Pool类
# class Pool(multiprocessing.Pool):
#     Process = NoDaemonProcess