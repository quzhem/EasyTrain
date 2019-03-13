from net.NetUtils import EasyHttp
from utils import AES
from utils.Log import Log


class Investment:
    baseUrl = None

    def __init__(self, baseUrl):
        Investment.baseUrl = baseUrl

    def login(self, username, pwd):
        baseUrl = Investment.baseUrl
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

    def QueryData(self, objectApiName, currentPage, pageSize=100, filters=[], orders=[]):
        baseUrl = Investment.baseUrl
        data = EasyHttp.post(baseUrl + '/api/metadata/data/getLayoutAndDataList',
                             {"currentPage": currentPage, "pageSize": pageSize, "moduleInfoId": objectApiName, 'filters': filters, "orders": orders});
        tableData = data['result']['data']['tableData']
        return tableData

    def UpdatePartData(self, data):
        baseUrl = Investment.baseUrl
        result = EasyHttp.post(baseUrl + '/api/metadata/data/updatePartData', data)
        if (result['code'] != 0):
            raise ValueError("更新失败 %s" % data)
