import requests
import time

from define.UserAgent import FIREFOX_USER_AGENT


def sendLogic(func):
    def wrapper(*args, **kw):
        for count in range(10):
            response = func(*args, **kw)
            if response:
                return response
            else:
                time.sleep(0.1)
        return None

    return wrapper


class EasyHttp(object):
    __session = requests.Session()

    @staticmethod
    def updateHeaders(headers):
        EasyHttp.__session.headers.update(headers)

    @staticmethod
    def resetHeaders():
        EasyHttp.__session.headers.clear()
        # EasyHttp.__session.headers.update({
        #     'Host': r'kyfw.12306.cn',
        #     'Referer': 'https://kyfw.12306.cn/otn/login/init',
        #     'User-Agent': FIREFOX_USER_AGENT,
        # })

    @staticmethod
    def setCookies(**kwargs):
        for k, v in kwargs.items():
            EasyHttp.__session.cookies.set(k, v)

    @staticmethod
    def removeCookies(key=None):
        EasyHttp.__session.cookies.set(key, None) if key else EasyHttp.__session.cookies.clear()

    @staticmethod
    # @sendLogic
    def send(urlInfo, params=None, data=None, skip=False, **kwargs):
        EasyHttp.resetHeaders()
        response = None
        if 'headers' in urlInfo and urlInfo['headers']:
            EasyHttp.updateHeaders(urlInfo['headers'])
        try:
            response = EasyHttp.__session.request(method=urlInfo['method'],
                                                  url=urlInfo['url'],
                                                  params=params,
                                                  data=data,
                                                  timeout=100,
                                                  allow_redirects=False,
                                                  **kwargs)
            if (skip != None and skip == True):
                return response
            if response.status_code == requests.codes.ok:
                if 'response' in urlInfo:
                    if urlInfo['response'] == 'binary':
                        return response.content
                    if urlInfo['response'] == 'html':
                        response.encoding = response.apparent_encoding
                        return response.text
                return response.json()
        except:
            pass
        return response

    def post(url, data=None, skip=False):
        return EasyHttp.send({'url': url, 'method': "POST", 'headers': {'Content-Type': r'application/json; charset=UTF-8'}}, json=data, skip=skip)

    def get(url, params=None, skip=False):
        result = EasyHttp.send({'url': url, 'method': "GET", 'headers': {'Content-Type': r'application/json; charset=UTF-8'}},
                               params=params, skip=skip);
        return result


if __name__ == '__main__':
    pass
