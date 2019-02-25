from utils.Log import Log
from net.NetUtils import EasyHttp
from utils import AES
import webbrowser


def queryComanyData(baseUrl):
    data = EasyHttp.post(baseUrl + '/api/metadata/data/getLayoutAndDataList',
                         {"currentPage": 1, "pageSize": 10, "moduleInfoId": "company"});
    Log.v("查询数据结果 %s" % data)


def login(baseUrl, username, pwd):
    secertKeyJSON = EasyHttp.get(baseUrl + '/api/org/account/getSecretKey')
    secertKey = secertKeyJSON['result']

    passwod = AES.encrypt_aes(pwd, secertKey)
    loginResultResponse = EasyHttp.post(baseUrl + '/api/org/account/login',
                                        {"secretKey": secertKey, "account": username, "password": passwod, "challenge": "",
                                         "validate": "", "seccode": "", "geetestSkip": False}
                                        , True)
    Log.v("登录成功!")
    # saas_login_token = None
    # for cookie in loginResultResponse.cookies:
    #     if (cookie.name == 'saas_login_token'):
    #         saas_login_token = cookie.value

    # EasyHttp.setCookies(loginResultResponse.cookies)
    # Log.v("loginResultJSON %s" % saas_login_token)


def visitInsight():
    data = EasyHttp.get('https://zjpark.jingdata.com/api/metadata/insight/zj/login')
    Log.v("返回token结果 %s" % data['result']['token'])
    webbrowser.open("http://insight.jingdata.com/api/redirect?redirect_url=%s&token=%s" % (
        'http://insight.jingdata.com', data['result']['token']))


def main():
    baseUrl = "https://zjpark.jingdata.com"
    # toDingding("ss");
    login(baseUrl, '13811138111', '123456')
    queryComanyData(baseUrl);
    visitInsight()


if __name__ == '__main__':
    main()
