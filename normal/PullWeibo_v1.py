from typing import Any

from utils.Log import Log
from net.NetUtils_v1 import EasyHttp
from pyquery import PyQuery as pq
import re
import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from utils import Utils

es = Elasticsearch()

esIndexName = 'weibo_data'


def createEsIndex():
    es.indices.create(index=esIndexName, ignore=400)
    Log.v("es index %s create finished!!" % esIndexName)


# 断点续传
def breakPointStart():
    categorys = getCategorys()
    res = es.search(index=esIndexName, body={"size": 0, "aggs": {"popular_colors": {"terms": {"size": 1000, "field": "typeStr.keyword"}}}})
    buckets = res['aggregations']['popular_colors']['buckets']

    if (buckets):
        bucketKeys = [x['key'] for x in buckets]
        categorys = [x for x in categorys if x['name'] not in bucketKeys]
        if (len(categorys) > 0):
            Log.v("需要断点续传，分类，%s" % categorys)
            start(categorys)


# 填充异常的数据
def fillingData():
    categorys = getCategorys()
    res = es.search(index=esIndexName, body={"size": 0, "aggs": {"popular_colors": {"terms": {"size": 1000, "field": "typeStr.keyword"}}}})
    buckets = res['aggregations']['popular_colors']['buckets']
    if (buckets):
        bucketsDict = {x['key']: x['doc_count'] for x in buckets}
        needCategorys = []
        for category in categorys:
            name = category['name']
            url = category['url']
            docCount = bucketsDict.get(name)
            detailCount = getMaxDataCount(url)
            if (docCount == None or docCount >= detailCount):
                continue
            needCategorys.append(category)
        if (len(needCategorys) > 0):
            Log.v("需要补充数据，需要处理的category,%s" % needCategorys)
            start(needCategorys)


# 拉取微博名人
def start(categorys=None):
    categorys = getCategorys() if categorys == None else categorys
    if (categorys == None):
        raise ValueError("categorys 不能为空")
    doWithCategory(categorys)


def setHeader():
    EasyHttp.updateHeaders(
        {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'
            ,
            # 'Cookie': 'Ugrow-G0=6fd5dedc9d0f894fec342d051b79679e; SUB=_2AkMqX8Oaf8PxqwJRmPAdxW3iao92yQ_EieKcAzJBJRMxHRl-yj83qkBStRB6Ad_tdaLYYuPA3WInHmG5MXvPWatJVLAT; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9Wh_xQjrkMWQwHVyvXhYTE9l'
            'Cookie': 'SINAGLOBAL=6645351159341.518.1560497330903; SSOLoginState=1561516705; _s_tentry=login.sina.com.cn; UOR=,,login.sina.com.cn; YF-V5-G0=a5a6106293f9aeef5e34a2e71f04fae4; Apache=7899497346251.21.1561516707296; ULV=1561516707343:2:2:1:7899497346251.21.1561516707296:1560497330920; wvr=6; wb_view_log_6450397269=1440*9002; SCF=AvzGWzuF9PzTIxqQj-ySDjFXS2dr9xX2gqFSxvkXUJhlY_CHrVGzFFew1bARZsTovqOpd5jl2JPe1N5mhKT0TPI.; SUB=_2A25wEfdGDeRhGeBK7lIS-SnOzTWIHXVTZ2-OrDV8PUJbmtBeLUbDkW9NR6dIewfUEHJMMYxaxmF1e9pkfBLryi7n; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5jT9JfjaFQ___AY-E5Ven65JpX5K-hUgL.FoqXSK501KMESo.2dJLoI79pUPDQ9gXt; SUHB=0QS5Z8JKX_3ZyY; ALF=1593227920; webim_unReadCount=%7B%22time%22%3A1561691945331%2C%22dm_pub_total%22%3A0%2C%22chat_group_pc%22%3A0%2C%22allcountNum%22%3A49%2C%22msgbox%22%3A0%7D; YF-Page-G0=96c3bfa80dc53c34a567607076bb434e|1561691950|1561691919'
        }
    )


def doWithCategory(categorys):
    for category in categorys:
        name = category['name']
        url = category['url']
        maxPageSize = getMaxPageSize(url)
        if (maxPageSize == 0):
            continue
        for page in range(1, maxPageSize + 1):
            pullAndSave(name, page, url)


@Utils.funcWithTry()
def pullAndSave(name, page, url):
    Log.v("当前处理第%s页，url %s" % (page, url))
    userInfoList = findUserInfoList(url, page)
    for userInfo in userInfoList:
        userInfo.typeStr = name
        pullUserInfo(userInfo)
    save(userInfoList)


@Utils.funcWithTry()
def pullUserInfo(userInfo):
    pullDetailInfo(userInfo)
    pullMoreInfo(userInfo)


def getCategorys():
    data = EasyHttp.get("https://d.weibo.com/1087030002_2975_1003_0")
    doc = pq(data.content)
    homeScript = None
    result = []
    for script in doc.find('script'):
        if (script.text.find('"domid":"Pl_Discover_TextNewList__3"') > 0):
            homeScript = script.text
            break

    homeScriptHtml = findScriptMatchHtml(homeScript)
    doc = pq(homeScriptHtml)
    showLiTag = doc.find('.item_box li,.subitem_box li')
    # showLiTag.append(doc.find('.subitem_box li'))
    start = False
    for index in range(0, showLiTag.length - 1):
        liTag = showLiTag.eq(index)
        url = liTag.find("a").attr("href")
        url = url.replace("#", "")
        if (liTag.has_class("current")):
            start = True
        if (start and url != 'javascript:void(0);'):
            result.append({'name': liTag.find('.item_title').text(), 'url': url})
    return result


@Utils.funcWithTry()
def save(userInfoList):
    dictList = [{
        "_id": x.id, "_index": esIndexName, "_type": "company_info", '_source': x.__dict__} for x in
        userInfoList]
    # res = es.bulk(index=esIndexName, doc_type='weibo', body=dictList)
    res = helpers.bulk(es, dictList)
    Log.v("insert es data: %s,result %s" % ([x.__dict__ for x in userInfoList], res))


# 从详情页中找到更多的地址
# def pullDetailInfo(userInfoList):
#     for userInfo in userInfoList:
#         data = EasyHttp.get("https:%s" % userInfo.detailUrl)
#
#         matchObj = re.match(r'[\s\S]*CONFIG\[\'page_id\'\]=\'(.*)\'[\s\S]*', data.content.decode('utf-8'), re.M | re.I)
#
#         # detailInfoDoc = pq(data.content)
#         # allScript = detailInfoDoc('script')
#         # homeFeedScript = str
#         # for script in allScript:
#         #     if (script.text.find('"domid":"Pl_Core_UserInfo__6"') > 0):
#         #         homeFeedScript = script.text
#         #         break
#         # # 简介相关信息
#         # homeFeedScriptHtml = findScriptMatchHtml(homeFeedScript)
#         # homeFeedDoc = pq(homeFeedScriptHtml)
#         # a = homeFeedDoc.find(".WB_cardmore")
#         userInfo.infoMoreUrl = "//weibo.com/p/%s/info?mod=pedit_more" % matchObj.group(1)
#         break

def pullDetailInfo(userInfo):
    data = EasyHttp.get("https:%s" % userInfo.detailUrl)
    # # 补充一部分信息
    detailInfoDoc = pq(data.content)
    allScript = detailInfoDoc('script')
    # triColumnScript = str
    homeFeedScript = str
    for script in allScript:
        # if (script.text.find('re_T8CustomTriColumn__3') > 0):
        #     triColumnScript = script.text
        #     continue
        if (script.text.find('"domid":"Pl_Core_UserInfo__6"') > 0):
            homeFeedScript = script.text
            break
    # # 关注、粉丝数区域
    # triColumnHtml = findScriptMatchHtml(triColumnScript)
    # # 简介相关信息
    homeFeedScriptHtml = findScriptMatchHtml(homeFeedScript)
    homeFeedDoc = pq(homeFeedScriptHtml)
    a = homeFeedDoc.find('.PCD_person_info .WB_cardmore')
    href = a.attr('href')
    if (href is not None and href != 'javascript:;'):
        userInfo.infoMoreUrl = href if str(href).startswith('//weibo.com') else "//weibo.com%s" % href
    else:
        # 查询更多的个人信息
        matchObj = re.match(r'[\s\S]*CONFIG\[\'page_id\'\]=\'(.*)\'[\s\S]*', data.content.decode('utf-8'), re.M | re.I)
        userInfo.infoMoreUrl = "//weibo.com/p/%s/info?mod=pedit_more" % matchObj.group(1)


# # 填充关注、粉丝数、微博数
# fillStars(userInfo, triColumnHtml)
# # 填充简介、地区、公司、出生日期
# fillIntroduction(userInfo, homeFeedScriptHtml)
# Log.v(userInfo)


def pullMoreInfo(userInfo):
    data = EasyHttp.get("https:%s" % userInfo.infoMoreUrl)
    detailInfoDoc = pq(data.content)
    allScript = detailInfoDoc('script')
    triColumnScript = None
    baseInfoScript = None
    for script in allScript:
        if (triColumnScript is not None and baseInfoScript is not None):
            break
        if (script.text.find('关注') > 0 and script.text.find('粉丝') > 0 and script.text.find('微博') > 0):
            triColumnScript = script.text
            continue
        if (script.text.find('基本信息') > 0 and script.text.find('注册时间') > 0):
            baseInfoScript = script.text
            continue
    if (triColumnScript != None):
        # 关注、粉丝数区域
        triColumnHtml = findScriptMatchHtml(triColumnScript)
        fillStars(userInfo, triColumnHtml)
    if (baseInfoScript != None):
        # 简介相关信息
        baseInfoScriptHtml = findScriptMatchHtml(baseInfoScript)
        # # 填充简介、地区、公司、出生日期
        fillIntroduction(userInfo, baseInfoScriptHtml)

    # Log.v(userInfo)


# 填充关注数、粉丝数和微博数
# < div class="WB_cardwrap S_bg2">tttttt < div class="PCD_counter">ttttttt < div class="WB_innerwrap">tttttttt < table class="tb_counter" cellpadding="0" cellspacing="0">ttttttttt < tbody>tttttttttt < tr>ttttttttttttttttttttt < td class="S_line1">tttttttttttttttttttttt < a bpfilter="page_frame" class="t_link S_txt1" href="//weibo.com/p/1004062039807581/follow?from=page_100406&wvr=6&mod=headfollow#place"> <strong class="W_f14">697 < /strong> <span class="S_txt2">关注</span></a> </td>tttttttttttttttttttttttttttttttt < td class="S_line1">tttttttttttttttttttttt < a bpfilter="page_frame" class="t_link S_txt1" href="//weibo.com/p/1004062039807581/follow?relate=fans&from=100406&wvr=6&mod=headfans&current=fans#place"> <strong class="W_f14">1867172 < /strong> <span class="S_txt2">粉丝</span></a> </td>tttttttttttttttttttttttttttttttt < td class="S_line1">tttttttttttttttttttttt < a bpfilter="page_frame" class="t_link S_txt1" href="//weibo.com/p/1004062039807581/home?from=page_100406_profile&wvr=6&mod=data#place"> <strong class="W_f14">259 < /strong> <span class="S_txt2">微博</span></a> </td>ttttttttttttttttttttttttttttttt < /tr>ttttttttt</tbody>tttttttt < /table>ttttttt</div>tttttt < /div>ttttt</div>
def fillStars(userInfo, html):
    doc = pq(html)
    allStrongTag = doc('.S_line1 strong')
    userInfo.stars = allStrongTag[0].text
    userInfo.fans = allStrongTag[1].text
    userInfo.weiboCount = allStrongTag[2].text


# 填充简介、地区、公司、出生日期
# t<div class="WB_cardwrap S_bg2" >rntttttt<div class="PCD_counter">rnttttttt<div class="WB_innerwrap">rntttttttt<table class="tb_counter" cellpadding="0" cellspacing="0">rnttttttttt<tbody>rntttttttttt<tr>rnttttttttttttttttttttt<td class="S_line1">rntttttttttttttttttttttt<a bpfilter="page_frame"class="t_link S_txt1" href="//weibo.com/p/1004062039807581/follow?from=page_100406&wvr=6&mod=headfollow#place" ><strong class="W_f14">697</strong><span class="S_txt2">关注</span></a></td>rntttttttttttttttttttttttttttttttt<td class="S_line1">rntttttttttttttttttttttt<a bpfilter="page_frame"class="t_link S_txt1" href="//weibo.com/p/1004062039807581/follow?relate=fans&from=100406&wvr=6&mod=headfans&current=fans#place" ><strong class="W_f14">1867406</strong><span class="S_txt2">粉丝</span></a></td>rntttttttttttttttttttttttttttttttt<td class="S_line1">rntttttttttttttttttttttt<a bpfilter="page_frame"class="t_link S_txt1" href="//weibo.com/p/1004062039807581/home?from=page_100406_profile&wvr=6&mod=data#place" ><strong class="W_f14">259</strong><span class="S_txt2">微博</span></a></td>rnttttttttttttttttttttttttttttttt</tr>rnttttttttt</tbody>rntttttttt</table>rnttttttt</div>rntttttt</div>rnttttt</div>rn
# 公司 <li class="li_1 clearfix"><span class="pt_title S_txt2">公司：</span>rn<span class="pt_detail">rn<a href="http://s.weibo.com/user/&amp;work=%E8%8A%92%E6%9E%9C%E6%8D%9E%E6%96%87%E5%8C%96%E4%BC%A0%E5%AA%92%E6%9C%89%E9%99%90%E5%85%AC%E5%8F%B8&amp;from=inf&amp;wvr=5&amp;loc=infjob" target="_blank">芒果捞文化传媒有限公司</a>rn<br/>rn职位：艺人部</span>rn</li>rnrnrn

def fillIntroduction(userInfo, html):
    doc = pq(html)
    allLiTag = doc('.clearfix li')
    switch = {
        "真实姓名": lambda x: userInfo.__setattr__('name', x),
        "所在地": lambda x: userInfo.__setattr__('address', x),
        "邮箱": lambda x: userInfo.__setattr__('email', x),
        "大学": lambda x, y: fillSpanATagUserInfo(userInfo, y, 'universityWbUrl', 'university'),
        "生日": lambda x: fillBirthday(userInfo, x),
        "标签": lambda x: userInfo.__setattr__('tag', x),
        "简介": lambda x: userInfo.__setattr__('brief', x),
        "公司": lambda x, y: fillSpanATagUserInfo(userInfo, y, 'companyWbUrl', 'company'),
        "星座": lambda x: userInfo.__setattr__('constellation', x),
        "注册时间": lambda x: userInfo.__setattr__('registrationTime', formatDate(x)),
        "性别": lambda x: userInfo.__setattr__('sex', x)
    }
    for index in range(0, allLiTag.length):
        spansDoc = allLiTag.eq(index)
        titleSpanDoc = spansDoc.find('.pt_title')
        detailSpanDoc = spansDoc.find('.pt_detail')
        title = format(titleSpanDoc.text()).replace("：", "")
        value = format(detailSpanDoc.text())
        func = switch.get(title, lambda x: x)
        # (func(value, detailSpanDoc), func(value))[func.__code__.co_argcount == 1]
        func(value) if func.__code__.co_argcount == 1 else func(value, detailSpanDoc)


def getSpanATag(spanDoc):
    a = spanDoc.find('a')
    if (a != None):
        return a.attr('href'), a.text()


def fillSpanATagUserInfo(userInfo, spanDoc, key1, key2):
    href, value = getSpanATag(spanDoc)
    userInfo.__setattr__(key1, href)
    userInfo.__setattr__(key2, value)


def fillBirthday(userInfo, dateStr):
    match = re.match(r'(\d*)年(\d*)月(\d*)日', dateStr, re.M | re.I);
    if (match != None and match.lastindex == 3):
        userInfo.birthday = formatDate(dateStr, "%Y年%m月%d日")
        userInfo.birthday_year = match.group(1)
        userInfo.birthday_month = match.group(2)
        userInfo.birthday_day = match.group(3)
        return

    match = re.match(r'(\d*)月(\d*)日', dateStr, re.M | re.I)
    if (match != None and match.lastindex == 2):
        userInfo.birthday_year = match.group(1)
        userInfo.birthday_month = match.group(2)
        return
    if ('座' in dateStr):
        userInfo.constellation = dateStr


def formatDate(dateStr, format="%Y-%m-%d"):
    try:
        return time.mktime(time.strptime(dateStr, format));
    except:
        Log.v("date format is err: %s,format: %s" % (dateStr, format))
        return None


@Utils.funcWithTry()
def findUserInfoList(url, page):
    data = EasyHttp.get("https:%s?page=%s#Pl_Core_F4RightUserList__4" % (url, page))
    listHtml = str(data.content, 'utf-8')
    finalDoc = findScriptUserList(listHtml)
    # 解析头像中的链接信息
    # rn < a
    # href = "//weibo.com/wangzhenq?refer_flag=1087030701_2975_1003_0"
    # title = "&#x4E3B;&#x6301;&#x738B;&#x4E0D;&#x51E1;"
    # target = "_blank" > rn < img
    # alt = "主持王不凡"
    # src = "//tvax1.sinaimg.cn/crop.0.0.1080.1080.50/7994fe5dly8fyojcnxcxtj20u00u0wga.jpg"
    # usercard = "id=2039807581&amp;refer_flag=1087030701_2975_1003_0"
    # width = "50"
    # height = "50" / > rn < / a > rn
    modPic = finalDoc('.mod_pic')
    userInfoList = []
    for mod in modPic:
        a = mod.find('a')
        a = pq(a)
        img = pq(a.find("img"))
        usercard = img.attr('usercard')
        id = (str(usercard).split('&')[0]).split('=')[1]
        # userInfoList.append({'id': id, 'img': img.attr('src'), 'url': a.attr('href'), 'name': a.attr('title')})
        userInfoList.append(UserInfo(id, a.attr('title'), a.attr('href'), img.attr('src')))
        # userInfoList.append(
        #     UserInfo('2943484181', a.attr('title'), '//weibo.com/guantou3000wen?refer_flag=1087030701_2975_1003_0', img.attr('src')))
        # break

    # Log.v(userInfoList)
    return userInfoList


def findScriptUserList(listHtml):
    doc = pq(listHtml)
    # 找到所有的script，并包含pl.content.signInPeople.index 的html内容
    scriptList = doc('script')
    scriptContent = None
    for script in scriptList:
        if (script.text.find('pl.content.signInPeople.index') > 0):
            scriptContent = script.text
    finalHtml = findScriptMatchHtml(scriptContent)
    finalDoc = pq(finalHtml)
    return finalDoc


@Utils.funcWithTry()
def getMaxPageSize(url):
    data = EasyHttp.get("https:%s" % (url))
    doc = findScriptUserList(data.content)
    pageTag = doc.find('.W_pages a[bpfilter="page"]')
    # 倒数第二个为总数
    aTag = pageTag.eq(pageTag.length - 2)
    return int(aTag.text()) if aTag.text() != '' else 0


# 获取指定url的数据总个数
@Utils.funcWithTry()
def getMaxDataCount(url):
    maxPageSize = getMaxPageSize(url)
    lastPageUserInfoList = findUserInfoList(url, maxPageSize)
    return (maxPageSize - 1) * 10 + len(lastPageUserInfoList)


def findScriptMatchHtml(scriptContent):
    matchObj = re.match(r'.*"html":"(.*)".*', scriptContent, re.M | re.I)
    finalHtml = matchObj.group(1)
    finalHtml = finalHtml.replace('  ', '').replace("\\r", "  ").replace("\\n", " ").replace("\\t", " ").replace('\\', '')
    return finalHtml


def format(arg):
    if (type(arg) == str):
        return arg.replace('rnttttttttttttttttttttttttt', '').replace('tttttttttttt', '').replace("rn", "").strip()
    return arg


class UserInfo:
    # id主属性
    id = None
    # 昵称
    nickname = None
    # 名称
    name = None
    # 邮箱
    email = None
    # 详情页地址
    detailUrl = None
    # 个人信息更多页面
    infoMoreUrl = None
    # 头像
    avatar = None
    # 关注数量
    stars = int
    # 粉丝数
    fans = int
    # 微博数
    weiboCount = int
    # 地区
    address = None
    # 性别
    sex = None
    # 公司
    company = None

    # 公司微博地址
    companyWbUrl = None
    # 大学
    university = None
    # 大学微博地址
    universityWbUrl = None

    # 出生日期
    birthday = int
    # 出生日期-年
    birthday_year = None
    # 出生日期-月
    birthday_month = None
    # 出生日期-日
    birthday_day = None

    # 星座
    constellation = None
    # 个人简介
    brief = None
    # 注册时间
    registrationTime = int

    # 标签
    tag = None
    # 标签微博地址
    tagWbUrl = None
    # 用户类型
    typeStr = None

    def __init__(self, id, nickname, url, avatar):
        self.id = id
        self.nickname = nickname
        self.detailUrl = url
        self.avatar = avatar

    def gatherAttrs(self):
        return ",".join("{}={}"
                        .format(k, getattr(self, k))
                        for k in self.__dict__.keys())
        # attrs = []
        # for k in self.__dict__.keys():
        #     item = "{}={}".format(k, getattr(self, k))
        #     attrs.append(item)
        # return attrs
        # for k in self.__dict__.keys():
        #     attrs.append(str(k) + "=" + str(self.__dict__[k]))
        # return ",".join(attrs) if len(attrs) else 'no attr'

    def __str__(self):
        return self.__dict__.__str__()
        # return "[{}:{}]".format(self.__class__.__name__, self.gatherAttrs())

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, format(value))


def migrateData():
    indexName = "weibo_data_new"
    es.indices.create(index=indexName, ignore=400, body={"settings": {"number_of_shards": 5}, "mappings": {"info": {
        "properties": {"birthday": {"type": "long"}, "birthday_day": {"type": "integer"}, "birthday_month": {"type": "integer"},
                       "birthday_year": {"type": "integer"}, "fans": {"type": "integer"}, "weiboCount": {"type": "integer"},
                       "stars": {"type": "integer"}, "sex": {"type": "keyword"}, "id": {"type": "keyword"}, "infoMoreUrl": {"type": "keyword"},
                       "detailUrl": {"type": "keyword"}, "companyWbUrl": {"type": "keyword"}, "avatar": {"type": "keyword"},
                       "registrationTime": {"type": "long"}}}}})
    data = helpers.scan(es,
                        # query={"query": {"match": {"title": "python"}}},
                        index="weibo_data",
                        # doc_type="books"
                        )

    def tu(x):
        source = x['_source']
        source['tag'] = str(source['tag']).split(' ') if 'tag' in source and source['tag'] != None else None
        x.update({'_index': indexName, '_type': "info", '_source': source})
        return x

    dictList = [tu(x) for x in data]

    res = helpers.bulk(es, dictList)
    Log.v("insert es result %s,data: %s" % (res, dictList))


def init():
    setHeader()
    createEsIndex()
    breakPointStart()
    fillingData()


def main():
    # init()
    migrateData()

    # start()


if __name__ == '__main__':
    main()
