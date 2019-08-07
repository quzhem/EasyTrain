import redis
from utils.Log import Log
import json
from elasticsearch import Elasticsearch, helpers
from utils.PgSql import PgSql
import cpca
# import cpca.resources
import re
from utils import Utils
from ins.NationArea import NationArea

key = 'MonitorData'

xMap = {
    'UserOrgObj': {
        'index_name': 'index_ic_user',
        'AddBefore': lambda x: doAddUserDataBefore(x),
        'mapping': {
            "mappings": {
                "info": {
                    "properties": {
                        "address": {
                            "type": "keyword"
                        },
                        "address_city": {
                            "type": "keyword"
                        },
                        "address_city_location": {
                            "type": "keyword"
                        },
                        "address_district": {
                            "type": "keyword"
                        },
                        "address_house": {
                            "type": "keyword"
                        },
                        "address_province": {
                            "type": "keyword"
                        },
                        "address_province_location": {
                            "type": "keyword"
                        },
                        "contacts": {
                            "type": "keyword"
                        },
                        "contacts_info": {
                            "type": "keyword"
                        },
                        "email": {
                            "type": "keyword"
                        },
                        "fax": {
                            "type": "keyword"
                        },
                        "id": {
                            "type": "keyword"
                        },
                        "is_del": {
                            "type": "keyword"
                        },
                        "login_name": {
                            "type": "keyword"
                        },
                        "org_phone": {
                            "type": "keyword"
                        },
                        "org_propertis": {
                            "type": "keyword"
                        },
                        "pwd": {
                            "type": "keyword"
                        },
                        "salt": {
                            "type": "keyword"
                        },
                        "state": {
                            "type": "keyword"
                        },
                        "remark": {
                            "type": "keyword"
                        },
                        "create_time": {
                            "type": "date"
                        },
                        "last_modified_time": {
                            "type": "date"
                        }
                    }
                }
            }
        },
        'allSql': "select * from ic_user a inner join ic_user_org b on a.id=b.id where a.is_del='0' order by a.create_time",
        'idFilterSql': "select * from ic_user a inner join ic_user_org b on a.id=b.id where a.is_del='0' and a.id in (%s) order by a.create_time"

    },
    'SignObj': {
        'index_name': 'index_ic_sign',
        'mapping': {
            "mappings": {
                "info": {
                    "properties": {
                        "address": {
                            "type": "keyword"
                        },
                        "batch": {
                            "type": "keyword"
                        },
                        "billing_id": {
                            "type": "keyword"
                        },
                        "contacts": {
                            "type": "keyword"
                        },
                        "contacts_info": {
                            "type": "keyword"
                        },
                        "create_time": {
                            "type": "date"
                        },
                        "email": {
                            "type": "keyword"
                        },
                        "entries_number": {
                            "type": "long"
                        },
                        "equipment_name": {
                            "type": "keyword"
                        },
                        "equipment_number": {
                            "type": "keyword"
                        },
                        "equipment_vender": {
                            "type": "keyword"
                        },
                        "equipment_version": {
                            "type": "keyword"
                        },
                        "fax": {
                            "type": "keyword"
                        },
                        "id": {
                            "type": "keyword"
                        },
                        "is_del": {
                            "type": "keyword"
                        },
                        "last_modified_time": {
                            "type": "date"
                        },
                        "last_operate": {
                            "type": "long"
                        },
                        "name": {
                            "type": "keyword"
                        },
                        "org_id": {
                            "type": "keyword"
                        },
                        "org_name": {
                            "type": "keyword"
                        },
                        "parm_type": {
                            "type": "keyword"
                        },
                        "queue_number": {
                            "type": "keyword"
                        },
                        "remark": {
                            "type": "keyword"
                        },
                        "sign_id": {
                            "type": "keyword"
                        },
                        "state": {
                            "type": "keyword"
                        },
                        "year": {
                            "type": "date",
                            "format": "yyyy"
                        }
                    }
                }
            }
        },
        'allSql': "select * from ic_sign a inner join ic_equipment b on a.id=b.id where a.is_del='0'  order by a.create_time",
        'idFilterSql': "select * from ic_sign a inner join ic_equipment b on a.id=b.id where a.is_del='0' and a.id in (%s) order by a.create_time"
    },
    'BillingObj': {
        'index_name': 'index_ic_billing',
        'mapping': {
            "mappings": {
                "info": {
                    "properties": {
                        "account_bank": {
                            "type": "keyword"
                        },
                        "account_bank_no": {
                            "type": "keyword"
                        },
                        "address": {
                            "type": "keyword"
                        },
                        "contact_info": {
                            "type": "keyword"
                        },
                        "contacts": {
                            "type": "keyword"
                        },
                        "create_time": {
                            "type": "date"
                        },
                        "created_by": {
                            "type": "keyword"
                        },
                        "email": {
                            "type": "keyword"
                        },
                        "id": {
                            "type": "keyword"
                        },
                        "invoice_add": {
                            "type": "keyword"
                        },
                        "invoice_tel": {
                            "type": "keyword"
                        },
                        "is_del": {
                            "type": "keyword"
                        },
                        "last_modified_by": {
                            "type": "keyword"
                        },
                        "last_modified_time": {
                            "type": "date"
                        },
                        "name": {
                            "type": "keyword"
                        },
                        "org_id": {
                            "type": "keyword"
                        },
                        "price": {
                            "type": "float"
                        },
                        "remark": {
                            "type": "keyword"
                        },
                        "rise_name": {
                            "type": "keyword"
                        },
                        "status": {
                            "type": "keyword"
                        },
                        "tax_number": {
                            "type": "keyword"
                        },
                        "ticket_content": {
                            "type": "keyword"
                        },
                        "type": {
                            "type": "keyword",
                        }
                    }
                }
            }
        },
        'allSql': "select * from ic_billing as a  where a.is_del='0' order by a.create_time",
        'idFilterSql': "select * from ic_billing as a  where a.is_del='0' and a.id in (%s) order by a.create_time "
    },
    'CertificateObj': {
        'index_name': 'index_ic_certificate',
        'mapping': {
            "mappings": {
                "info": {
                    "properties": {
                        "create_time": {
                            "type": "date"
                        },
                        "created_by": {
                            "type": "keyword"
                        },
                        "file_name": {
                            "type": "keyword"
                        },
                        "id": {
                            "type": "keyword"
                        },
                        "is_del": {
                            "type": "keyword"
                        },
                        "last_modified_by": {
                            "type": "keyword"
                        },
                        "last_modified_time": {
                            "type": "date"
                        },
                        "name": {
                            "type": "keyword"
                        },
                        "org_id": {
                            "type": "keyword"
                        },
                        "org_name": {
                            "type": "keyword"
                        },
                        "parm_type": {
                            "type": "keyword"
                        },
                        "time": {
                            "type": "date"
                        }
                    }
                }
            }
        },
        'allSql': "select * from ic_certificate as a  where a.is_del='0' order by a.create_time",
        'idFilterSql': "select * from ic_certificate as a  where a.is_del='0' and a.id in (%s) order by a.create_time "
    }
}
# 特殊的市到省的映射
pcMap = {
    "杭州市": "浙江省"
}
es = None
pgSql = None
nationArea = None
connection = None


def init():
    global es
    global pgSql
    global nationArea
    global connection
    Log.load(filename='../logs/Inspect.log')
    es = Elasticsearch()
    pgSql = PgSql('localhost', 'guns20190223', 'postgres', '123456')
    nationArea = NationArea(pgSql)
    connection = getRedisConnection()


def getRedisConnection():
    pool = redis.ConnectionPool(host='localhost', port=6379)
    red = redis.Redis(connection_pool=pool)
    return red


def createEsIndex(esIndexName, mapping):
    flag = es.indices.exists(esIndexName)
    if (not flag):
        es.indices.create(index=esIndexName, body=mapping)
        Log.v("es index %s create finished!!" % esIndexName)


def monitor():
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
        idFilterSql = config['idFilterSql']
        dataList = doQueryData(idFilterSql, idList)
        # dataList = config['queryData'](idFilterSql, idList)

    if ('AddBefore' in config and config['AddBefore'] is not None):
        dataList = config['AddBefore'](dataList)
    dictList = [{
        "_id": x['id'], "_index": indexName, "_type": "info", '_source': x} for x in dataList]
    createEsIndex(indexName, config['mapping'])
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


def doQueryData(sql, idList):
    idStr = ','.join(
        ["'" + x + "'" for x in idList])
    dataList = pgSql.select(sql % idStr)
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
        if (Utils.isNotEmpty(province)):
            provinceResult = nationArea.queryProvinceByLabel(province)
            if (Utils.isNotEmpty(provinceResult)):
                valueId = provinceResult['value']
                longitude, latitude = extractLocation(provinceResult)
                data['address_province_location'] = '#'.join([province, ','.join([longitude, latitude])])
                if (Utils.isNotEmpty(city)):
                    cityResult = nationArea.queryCityByLabel(city, valueId)
                    valueId = cityResult['value']
                    longitude, latitude = extractLocation(cityResult)
                data['address_city_location'] = '#'.join([city, ','.join([longitude, latitude])])

    # data['location'] = {
    #     "type": "point",
    #     "coordinates": [13.400544, 52.530286]
    # }
    # data['coordinate'] = "13.400544,52.530286"
    return dataList


def extractLocation(dbResult):
    properties = json.loads(dbResult['properties'])
    return str(properties['longitude']), str(properties['latitude'])


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
def syncData(objectName, config):
    page = 1
    pageSize = 20
    count = 0
    while (True):
        data = pgSql.select(config['allSql'], page,
                            pageSize)
        Log.v(data)
        if (data is None or len(data) == 0):
            break
        doAddData(objectName, dataList=data)
        page += 1
        count += len(data)
    Log.v("成功入库数据 %s, %s 条" % (objectName, count))


# 从数据库中同步所有的数据
def syncAllData():
    for k, v in xMap.items():
        syncData(k, v)


if __name__ == '__main__':
    init()
    syncAllData()
    monitor()
