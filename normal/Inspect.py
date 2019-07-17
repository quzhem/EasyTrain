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
        'index_name': 'index_ic_user'
    }
}


def createEsIndex(esIndexName):
    es.indices.create(index=esIndexName, ignore=400)
    Log.v("es index %s create finished!!" % esIndexName)


def monitor():
    connection = getConnection()
    switch = {
        'Add': lambda x, y: doAddData(x, y),
        'Update': lambda x, y: doAddData(x, y),
        'Delete': lambda x, y: doAddData(x, y)
    }
    while (True):
        # {'type':'Add、Update、Delete','objectName':'','_source':{'name':'报名'}}
        result = connection.blpop(key)
        obj = json.loads(result)
        type = obj['type']
        objectName = obj['objectName']
        _source = obj['_source']
        switch[type](objectName, _source)


def doAddData(objectName, _source):
    config = _indexMapping[objectName]
    indexName = config['index_name']
    createEsIndex(indexName)
    es.index(indexName, _source)


def getConnection():
    pool = redis.ConnectionPool(host='localhost', port=6379, db=1)
    red = redis.Redis(connection_pool=pool)
    return red


if __name__ == '__main__':
    monitor()
