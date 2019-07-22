import redis
from utils.Log import Log
import json
from elasticsearch import Elasticsearch, helpers
from utils.PgSql import PgSql
import cpca
import re
from utils import Utils

key = 'MonitorData'

es = Elasticsearch()
pgSql = PgSql('localhost', 'guns20190223', 'postgres', '123456')
mapping = {
    "mappings": {
        "info": {
            "properties": {
                "location": {
                    "type": "geo_shape"
                }
            }
        }
    }
}

xMap = {
    'UserOrgObj': {
        'index_name': 'index_ic_user',
        'queryData': lambda x: doQueryUserData(x),
        'AddBefore': lambda x: doAddUserDataBefore(x)
    }
}
# 特殊的市到省的映射
pcMap = {
    "杭州市": "浙江省"
}


def createEsIndex(esIndexName):
    flag = es.indices.exists(esIndexName)
    if (not flag):
        es.indices.create(index=esIndexName, body=mapping)
        Log.v("es index %s create finished!!" % esIndexName)


def monitor():
    connection = getRedisConnection()
    switch = {
        'Add': lambda x, y: doAddData(x, y),
        'Update': lambda x, y: doAddData(x, y),
        'Delete': lambda x, y: doDeleteData(x, y)
    }
    while (True):
        # "{\"type\":\"Add\",\"object_name\":\"IcUserObj\",\"info\":[{\"id\":\"1\",\"name\":\"报名\"}]}"
        # "{\"type\":\"Delete\",\"object_name\":\"IcUserObj\",\"info\":[{\"id\":\"1\",\"name\":\"报名\"}]}"
        # {'type':'Add、Update、Delete','object_name':'','info':[{'id':'1','name':'报名'}]}
        popData = connection.blpop(key)[1];
        result = str(popData, encoding="utf-8")
        Log.w("redis pop result %s" % result)
        obj = json.loads(result)
        type = obj['type']
        objectName = obj['object_name']
        _source = obj['info']
        switch[type](objectName, _source)


def doAddData(objectName, _source=None, dataList=None):
    config = xMap[objectName]
    indexName = config['index_name']
    if (_source != None):
        idList = [x['id'] for x in _source]
        config['queryData'](idList)

    if ('AddBefore' in config and config['AddBefore'] is not None):
        dataList = config['AddBefore'](dataList)
    dictList = [{
        "_id": x['id'], "_index": indexName, "_type": "info", '_source': x} for x in dataList]
    res = helpers.bulk(es, dictList)
    Log.v("insert es result %s ,data: %s" % (res, [x for x in dataList]))


def doDeleteData(objectName, _source):
    config = xMap[objectName]
    indexName = config['index_name']
    dataList = _source
    dictList = [{
        '_op_type': 'delete', "_id": x['id'], "_index": indexName, "_type": "info", '_source': x} for x in dataList]
    res = helpers.bulk(es, dictList)
    Log.v("delete es data: %s,result %s" % ([x for x in dataList], res))


def specialConvert(address):
    if (Utils.isEmpty(address)):
        return ''
    address = str(address)
    matchObj = re.match('(.*省)?.*市.*区', address, re.M | re.I)
    if (matchObj is not None):
        return address
    if (address.startswith("苏州")):
        return address.replace('苏州', '苏州市')
    if (address.startswith("海北州")):
        return address.replace('海北州', '海北藏族自治州')
    if (address.startswith("中路高科")):
        return '北京市' + address
    return address


def doQueryUserData(idList):
    idStr = ','.join(
        ["'" + x + "'" for x in idList])
    dataList = pgSql.select(
        "select * from ic_user a inner join ic_user_org b on a.id=b.id where a.is_del='0' and a.id in (%s)order by a.create_time" % idStr)
    return dataList


def doAddUserDataBefore(dataList):
    for data in dataList:
        address = data['address']
        name = data['name']

        address, city, df, house, province, district = extractAddress(address)
        # # 从名称中再提取一次
        if (province is None or province.strip() == ''):
            address = name + address if address != None else name
            address, city, df, house, province, district = extractAddress(address)

        if (not all([province])):
            Log.w("----------------------")
            Log.w("address %s" % address)
            Log.w("数据id %s" % data['id'])
            Log.w(df)
            Log.w("----------------------")
        data['address_province'] = province
        data['address_city'] = city
        data['address_district'] = district
        if (isNaN(house) is not True):
            data['address_house'] = house
        data['location'] = {
            "type": "point",
            "coordinates": [13.400544, 52.530286]
        }
    return dataList


def extractAddress(address):
    address = specialConvert(address)
    df = cpca.transform([address], cut=False)
    dict = df.to_dict('index')[0]
    province = dict['省']
    city = dict['市']
    district = dict['区']
    house = str(dict['地址'])
    if (Utils.isEmpty(district) and '区' in house):
        # 高新开发区特殊的新区
        arr = str(house).split('区')
        district = arr[0] + '区'
        house = arr[1]
    if (pcMap.get(city) != None and pcMap.get(city) != province):
        province = pcMap[city]
    return address, city, df, house, province, district


def isNaN(num):
    return num != num


# 从数据库中同步用户数据
def syncUserData():
    createEsIndex(xMap['UserOrgObj']['index_name'])
    page = 1
    pageSize = 20
    count = 0
    while (True):
        data = pgSql.select("select * from ic_user a inner join ic_user_org b on a.id=b.id where a.is_del='0' order by a.create_time", page, pageSize)
        Log.v(data)
        if (data is None or len(data) == 0):
            break
        doAddData('UserOrgObj', dataList=data)
        page += 1
        count += len(data)
    Log.v("成功入库用户数据 %s 条" % count)


# 从数据库中同步所有的数据
def syncAllData():
    syncUserData()


def getRedisConnection():
    pool = redis.ConnectionPool(host='localhost', port=6379)
    red = redis.Redis(connection_pool=pool)
    return red


if __name__ == '__main__':
    doQueryUserData(['11'])
    Log.load(filename='../logs/Inspect.log')
    syncAllData()
    monitor()
