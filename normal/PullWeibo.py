from typing import Any

from utils.Log import Log
from net.NetUtils_v1 import EasyHttp
from pyquery import PyQuery as pq
import re
import time


# 拉取微博名人
def query():
    EasyHttp.updateHeaders(
        {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'
            ,
            'Cookie': 'Ugrow-G0=6fd5dedc9d0f894fec342d051b79679e; SUB=_2AkMqX8Oaf8PxqwJRmPAdxW3iao92yQ_EieKcAzJBJRMxHRl-yj83qkBStRB6Ad_tdaLYYuPA3WInHmG5MXvPWatJVLAT; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9Wh_xQjrkMWQwHVyvXhYTE9l'
        }
    )
    for page in range(1, 2):
        Log.v("当前处理第%s页" % page)
        data = EasyHttp.get("https://d.weibo.com/1087030002_2975_1003_0?page=%s#Pl_Core_F4RightUserList__4" % page)
        content = str(data.content, 'utf-8')
        userInfoList = findUserInfoList(content)
        pullDetailInfo(userInfoList)
        pullMoreInfo(userInfoList)


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

def pullDetailInfo(userInfoList):
    for userInfo in userInfoList:
        data = EasyHttp.get("https:%s" % userInfo.detailUrl)
        # 查询更多的个人信息
        matchObj = re.match(r'[\s\S]*CONFIG\[\'page_id\'\]=\'(.*)\'[\s\S]*', data.content.decode('utf-8'), re.M | re.I)
        userInfo.infoMoreUrl = "//weibo.com/p/%s/info?mod=pedit_more" % matchObj.group(1)
        # 补充一部分信息
        detailInfoDoc = pq(data.content)
        allScript = detailInfoDoc('script')
        triColumnScript = str
        homeFeedScript = str
        for script in allScript:
            if (script.text.find('re_T8CustomTriColumn__3') > 0):
                triColumnScript = script.text
                continue
            if (script.text.find('Pl_Core_UserInfo__6') > 0):
                homeFeedScript = script.text
                continue
        # 关注、粉丝数区域
        triColumnHtml = findScriptMatchHtml(triColumnScript)
        # 简介相关信息
        homeFeedScriptHtml = findScriptMatchHtml(homeFeedScript)
        # 填充关注、粉丝数、微博数
        fillStars(userInfo, triColumnHtml)
        # 填充简介、地区、公司、出生日期
        fillIntroduction(userInfo, homeFeedScriptHtml)
        Log.v(userInfo)
        break


def pullMoreInfo(userInfoList):
    for userInfo in userInfoList:
        data = EasyHttp.get("https:%s" % userInfo.infoMoreUrl)
        detailInfoDoc = pq(data.content)
        allScript = detailInfoDoc('script')
        # triColumnScript = str
        baseInfoScript = str
        for script in allScript:
            # if (script.text.find('关注') > 0 and script.text.find('粉丝') > 0 and script.text.find('微博') > 0):
            #     triColumnScript = script.text
            #     continue
            if (script.text.find('基本信息') > 0 and script.text.find('注册时间') > 0):
                baseInfoScript = script.text
                continue
        # 关注、粉丝数区域
        # triColumnHtml = findScriptMatchHtml(triColumnScript)
        # 简介相关信息
        homeInfoScriptHtml = findScriptMatchHtml(baseInfoScript)

        matchObj = re.match(
            r'[\s\S]*<span class="pt_title S_txt2">.*：</span><span class="pt_detail">([^<]*)*</span>[\s\S]*'
            ,
            homeInfoScriptHtml, re.M | re.I)
        Log.v(matchObj.group(1))

        # # 填充关注、粉丝数、微博数
        # fillStars(userInfo, triColumnHtml)
        # # 填充简介、地区、公司、出生日期
        # fillIntroduction(userInfo, homeInfoScriptHtml)
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
def fillIntroduction(userInfo, html):
    doc = pq(html)
    allStrongTag = doc('.ul_detail .item_text')
    userInfo.address = format(allStrongTag[0].text)
    for index in range(1, allStrongTag.length - 1):
        tag = allStrongTag[index]
        tagDoc = pq(tag)
        span = tagDoc.find('span')
        if (span.length > 0):
            spanText = span.text()
            a = tagDoc.find('a')
            if ('毕业' in spanText):
                userInfo.university = a.text()
                userInfo.universityUrl = a.attr('href')
                continue
            if ('公司' in spanText):
                userInfo.company = a.text()
                userInfo.companyWbUrl = a.attr('href')
                continue
            if ('标签' in spanText):
                userInfo.tag = a.text()
                userInfo.tagWbUrl = a.attr('href')
                continue
        else:
            tagText = format(tag.text)
            if (len(tagText) <= 0):
                continue
            if ('年' in tagText and '月' in tagText):
                userInfo.birthday = time.mktime(time.strptime(tagText, "%Y年%m月%d日"))
                continue
            if ('座' in tagText and len(tagText) == 3):
                # 星座
                userInfo.constellation = tagText
                continue
            # 匹配不上的，应该是简介
            userInfo.brief = format(tagText)


def findUserInfoList(content):
    doc = pq(content)
    # 找到所有的script，并包含pl.content.signInPeople.index 的html内容
    scriptList = doc('script')
    scriptContent = None
    for script in scriptList:
        if (script.text.find('pl.content.signInPeople.index') > 0):
            scriptContent = script.text
    finalHtml = findScriptMatchHtml(scriptContent)
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
    finalDoc = pq(finalHtml)
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

    # Log.v(userInfoList)
    return userInfoList


def findScriptMatchHtml(scriptContent):
    matchObj = re.match(r'.*"html":"(.*)".*', scriptContent, re.M | re.I)
    finalHtml = matchObj.group(1)
    finalHtml = finalHtml.replace('  ', '').replace('\\', '')
    return finalHtml


def format(arg):
    if (type(arg) == str):
        return arg.replace('rnttttttttttttttttttttttttt', '').replace('tttttttttttt', '')
    return arg


class UserInfo:
    # id主属性
    id = None
    # 名称
    name = None
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
    # 星座
    constellation = None
    # 个人简介
    brief = None
    # 标签
    tag = None
    # 标签微博地址
    tagWbUrl = None

    def __init__(self, id, name, url, avatar):
        self.id = id
        self.name = name
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
        return "[{}:{}]".format(self.__class__.__name__, self.gatherAttrs())

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, format(value))


def main():
    query()


if __name__ == '__main__':
    main()
