from utils.Log import Log
from net.NetUtils import EasyHttp
import re
import webbrowser


def query():
    for page in range(67, 1, -1):
        Log.v("当前处理第%s页" % page)
        data = EasyHttp.get("http://zrzyj.shenyang.gov.cn/sygtzyj/tdjy/glist%s.html" % page)
        if (data.status_code == 200):
            content = str(data.content, 'utf-8');
            # matchObj = re.match(r' <td .*><a target="_blank" href=".*">.*</a></td>', content, re.M | re.I)
            pattern = re.compile(r' <td .*><a target="_blank" href=".*">.*</a></td>')  # 查找数字
            hrefList = pattern.findall(content)
            for ahref in hrefList:
                matchObj = re.match(r'.*href="(.*)".*', ahref, re.M | re.I)
                url = matchObj.group(1)
                infoData = EasyHttp.get(url)
                if (infoData.status_code == 200):
                    infoContext = str(infoData.content, 'utf-8');
                    # infoMatch = re.match(r'.*<DIV style="WIDTH:.*" id=zoom class=nr_nr>(.*)<TABLE.*', infoContext, re.M | re.I | re.S)  # 查找数字
                    # infoTrimText = infoMatch.group(1)
                    if (infoContext.find("辽宁省城乡建设集团有限责任公司大股东") > 0):
                        # if (infoContext.find("土地拍卖公告沈土拍[2012]第69号") > 0 and infoContext.find("2012-012") >= 0):
                        Log.v(url)
                        webbrowser.open(url)


def main():
    query()


if __name__ == '__main__':
    main()
