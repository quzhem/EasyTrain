from utils.Log import Log
from net.NetUtils import EasyHttp
from normal.Investment import Investment

investment = None


# 维护公司的最新融资轮次
def updateCompany():
    currentPage = 1
    count = 0
    while (True):
        tableData = investment.QueryData('company', currentPage, orders=[{"fieldName": "created_at", "asc": False}])
        if (len(tableData) <= 0):
            break
        for data in tableData:
            id = data['id']
            count += 1
            invDataList = investment.QueryData('investment', 1,
                                               filters=[{"fieldName": "object_name", "operator": "EQ", "fieldValues": ['company']},
                                                        {"fieldName": "data_name", "operator": "EQ", "fieldValues": [id]}],
                                               orders=[{"fieldName": "finance_date", "asc": False}])
            if (len(invDataList) > 0):
                invData = invDataList[0]
                investment.UpdatePartData(
                    {"objectData": {"map": {"object_describe_api_name": "%s" % invData['object_describe_api_name'], "id": "%s" % invData['id'],
                                            "phase": "%s" % invData['phase']}}})

            Log.v("数据公司id %s,名称 %s" % (id, data['name']))

        currentPage += 1
    Log.v("成功处理数据条数 %s" % count)


# 重新关联洞见数据
def repeatRelate():
    currentPage = 1
    count = 0
    while (True):
        # 查询为空的，需要重新关联的公司
        tableData = investment.QueryData('company', currentPage,
                                         filters=[{"fieldName": "insight_related_id__u", "operator": "IS", "fieldValues": ["ff"]}],
                                         # filters=[{"fieldName": "insight_related_id__u", "operator": "IS", "fieldValues": ["gg"]}],
                                         orders=[
                                             {"fieldName": "created_at", "asc": False}])
        if (len(tableData) <= 0):
            break
        for data in tableData:
            id = data['id']
            name = data['name']
            Log.v("数据公司id %s,名称 %s" % (id, data['name']))
            if (data['insight_related_id__u'] is not None):
                # 先取消关联
                EasyHttp.post(Investment.baseUrl + '/api/metadata/insight/relatedInsightData',
                              {"describeApiName": "company", "dataId": "%s" % id, "insightRelatedObject": "%s" % name,
                               "insightRelatedObjectId": ""}
                              )

            insight_related_id = None
            count += 1
            keyWordResult = EasyHttp.post(Investment.baseUrl + '/api/metadata/insight/search/word', {"keyWord": "%s" % name})
            keyWordList = keyWordResult['result']['list']

            for keyWordData in keyWordList:
                if (keyWordData['short_name'] == name):
                    insight_related_id = keyWordData['id']
                    break
            # 关联精准数据
            relateResult = EasyHttp.post(Investment.baseUrl + '/api/metadata/insight/relatedInsightData',
                                         {"describeApiName": "company", "dataId": "%s" % id, "insightRelatedObject": "%s" % name,
                                          "insightRelatedObjectId": "%s" % insight_related_id}
                                         )
            if (relateResult['code'] != 0):
                Log.v("关联失败 %s" % relateResult)
                # raise ValueError("关联失败 %s" % relateResult)

        currentPage += 1
    Log.v("成功处理数据条数 %s" % count)


# 更新公司新增的字段，如最新估值、最新市值
def updateCompanyNewField():
    currentPage = 1
    count = 0
    while (True):
        tableData = investment.QueryData('company', currentPage,
                                         filters=[{"fieldName": "stock_code", "operator": "IS", "fieldValues": ["rr"], "subFieldName": None},
                                                  {"fieldName": "latest_market_value", "operator": "ISN", "fieldValues": ["33"],
                                                   "subFieldName": "num"}],
                                         orders=[{"fieldName": "created_at", "asc": False}])
        if (len(tableData) <= 0):
            break
        for data in tableData:
            id = data['id']
            count += 1
            # 更新已上市的公司，最新市值为空
            investment.UpdatePartData(
                {"objectData": {"map": {"object_describe_api_name": "company", "id": "%s" % id,
                                        "latest_market_value": None}}})

            Log.v("数据公司id %s,名称 %s" % (id, data['name']))

            currentPage += 1
        Log.v("成功处理数据条数 %s" % count)


def main():
    global investment
    # investment = Investment('https://investment-dev.jingdata.com')
    # investment.login('13800138000', '123456')
    investment = Investment('https://zjpark.jingdata.com')
    investment.login('13811138111', '123456')
    # updateCompany()
    repeatRelate()
    # updateCompanyNewField()


if __name__ == '__main__':
    main()
