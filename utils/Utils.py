import urllib.parse

from datetime import datetime

from utils.Log import Log
import time


def urldeocde(str):
    return urllib.parse.unquote(str)


def check(target, log):
    if not target:
        Log.e(log)
        return False
    return True


def formatDate(date):
    return datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')


def funcWithTry(count=10):
    def func(func):
        def wrapper(*args, **kw):
            for i in range(count):
                try:
                    result = func(*args, **kw)
                    return result
                except Exception as e:
                    Log.e("方法%s执行出错，进行重试,参数 %s,%s" % (func, args, kw), e)
                    time.sleep(0.1)
            return None

        return wrapper

    return func


def isEmpty(obj):
    return obj == None or str(obj).strip() == ''


def isNotEmpty(obj):
    return not isEmpty(obj)


@funcWithTry()
def ceshi(a='11'):
    Log.v("ceshi")
    return 'ceshi'
    # raise ValueError('0')


if __name__ == '__main__':
    print(formatDate('20180102'))
    Log.v(ceshi('22'))
