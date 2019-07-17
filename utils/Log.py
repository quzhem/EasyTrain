from colorama import Fore
import json
import traceback


class Log(object):
    def __print(msg, color):
        if type(msg) == str:
            print(color + msg + Fore.RESET)
        elif isinstance(msg, Exception):
            Log.__print(traceback.format_exc(), Fore.RED)
        else:
            print(color)
            print(msg)
            print(Fore.RESET)

    def d(msg):
        Log.__print(msg, Fore.BLUE)

    def v(msg):
        Log.__print(msg, Fore.GREEN)

    def w(msg):
        Log.__print(msg, Fore.YELLOW)

    def e(msg):
        Log.__print(msg, Fore.RED)

    def e(msg, e):
        Log.__print(msg + ",%s" % traceback.format_exc(), Fore.RED)


if __name__ == '__main__':
    pass
