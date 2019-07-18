import redis
from utils.Log import Log
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

key = 'MonitorData'

es = Elasticsearch()
esIndexName = 'weibo_data'

_indexMapping = {
    'IcUserObj': {
        'index_name': 'index_ic_user',
        'AddBefore': lambda x: doAddUserDataBefore(x)
    }
}


def createEsIndex(esIndexName):
    es.indices.create(index=esIndexName, ignore=400)
    Log.v("es index %s create finished!!" % esIndexName)


def monitor():
    connection = getRedisConnection()
    switch = {
        'Add': lambda x, y: doAddData(x, y),
        'Update': lambda x, y: doAddData(x, y),
        'Delete': lambda x, y: doDeleteData(x, y)
    }
    while (True):
        # {"type":"Add","objectName":"IcUserObj","_source":[{"id":"1","name":"报名"}]}
        # {'type':'Add、Update、Delete','objectName':'','_source':[{'id':'1','name':'报名'}]}
        popData = connection.blpop(key)[1];
        result = str(popData, encoding="utf-8")
        Log.v("redis pop result %s" % result)
        obj = json.loads(result)
        type = obj['type']
        objectName = obj['objectName']
        _source = obj['_source']
        switch[type](objectName, _source)


def doAddData(objectName, _source):
    config = _indexMapping[objectName]
    indexName = config['index_name']
    createEsIndex(indexName)

    dataList = _source
    if ('AddBefore' in config and config['AddBefore'] is not None):
        dataList = config['AddBefore'](dataList)
    dictList = [{
        "_id": x['id'], "_index": indexName, "_type": "info", '_source': x} for x in dataList]
    res = helpers.bulk(es, dictList)
    Log.v("insert es data: %s,result %s" % ([x for x in dataList], res))


def doDeleteData(objectName, _source):
    config = _indexMapping[objectName]
    indexName = config['index_name']
    dataList = _source
    dictList = [{
        '_op_type': 'delete', "_id": x.id, "_index": indexName, "_type": "info", '_source': dataList} for x in dataList]
    res = helpers.bulk(es, dictList)
    Log.v("delete es data: %s,result %s" % ([x for x in dataList], res))


def doAddUserDataBefore(dataList):
    return dataList


def getRedisConnection():
    pool = redis.ConnectionPool(host='localhost', port=6379)
    red = redis.Redis(connection_pool=pool)
    return red


if __name__ == '__main__':
    monitor()
