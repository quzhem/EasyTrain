from logging.handlers import TimedRotatingFileHandler

from colorama import Fore, Style
# import json
# import traceback
import logging


class CustomFormatter(logging.Formatter):
    default_msec_format = '%s.%03d'

    switch = {
        logging.DEBUG: lambda x: Fore.BLUE + x,
        logging.INFO: lambda x: Fore.GREEN + x,
        logging.WARN: lambda x: Fore.YELLOW + x,
        logging.ERROR: lambda x: Fore.RED + x
    }

    def __init__(self, fmt=None, datefmt=None, style=None):
        self._style = style
        self._fmt = fmt
        self.datefmt = datefmt

    def formatMessage(self, record):
        msg = logging.Formatter.formatMessage(self, record)
        return self.switch[record.levelno](msg)


class Log(object):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] - %(message)s')
    for hander in logging.root.handlers:
        formatter = hander.formatter
        hander.setFormatter(CustomFormatter(formatter._fmt, formatter.datefmt, formatter._style))

    def load(level=logging.INFO, filename=None):
        logging.root.setLevel(level=level)
        if (filename != None):
            th = TimedRotatingFileHandler(filename=filename, when='D', backupCount=7, encoding='utf-8')
            # 往文件里写入#指定间隔时间自动生成文件的处理器
            # 实例化TimedRotatingFileHandler
            # interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
            # S 秒
            # M 分
            # H 小时、
            # D 天、
            # W 每星期（interval==0时代表星期一）
            # midnight 每天凌晨
            th.setFormatter(logging.Formatter(fmt='%(asctime)s [%(levelname)s] - %(message)s'))  # 设置文件里写入的格式
            logging.root.addHandler(th)

    def d(msg):
        logging.debug(msg)

    def v(msg):
        logging.info(msg)

    def w(msg):
        logging.warning(msg)

    def e(msg, e=None):
        if (e != None):
            logging.error(msg, exc_info=True, stack_info=True)
        else:
            logging.error(msg)

    def debug(msg):
        logging.debug(msg)

    def info(msg):
        logging.info(msg)

    def warn(msg):
        logging.warning(msg)

    def error(msg, e=None):
        Log.e(msg, e)


if __name__ == '__main__':
    Log.load(logging.DEBUG, filename="../logs/ceshi.log")
    Log.d("DEBUG")
    Log.v("INFO")
    Log.w("WARN")
    Log.e("ERROR")
    Log.debug("DEBUG")
    Log.info("INFO")
    Log.warn("WARN")
    Log.error("ERROR")
    # Log.error("ERROR", ValueError("试试"))
