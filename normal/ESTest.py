from elasticsearch import Elasticsearch
from utils.Log import Log


def test1():
    es = Elasticsearch(hosts="http://readonly:B6Y7WAYrrIrF@es-cn-459112rhg00084zyb.elasticsearch.aliyuncs.com:9200",
                       usename='readonly', password='B6Y7WAYrrIrF')
    pingResult = es.ping()
    res = es.search(index='ams_pro_reg_company_investment', body={})
    Log.v("ping 结果:%s" % pingResult)


if __name__ == '__main__':
    test1()
