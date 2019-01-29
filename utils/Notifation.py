# 提醒服务，如钉钉、邮件等
import requests
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from utils.Log import Log


def notify(message):
    toDingding(message)
    toEmail(message)


def toDingding(message):
    __session = requests.Session()

    formData = {
        "msgtype": "text",
        "text": {
            "content": "" + message
        },
        "at": {
            "atMobiles": [
                "18613371416"
            ],
            "isAtAll": False
        }
    }
    urlInfo = {'url': r'https://oapi.dingtalk.com/robot/send?access_token=8bea4bc82161922920d6529b0029e11ed62b2cb00a8c1f48eaf8ae1ed08fdab8'
        , 'method': 'POST',
               'headers': {
                   'Content-Type': r'application/json; charset=UTF-8'
               },
               }
    __session.headers.update(urlInfo['headers'])
    __session.request(method=urlInfo['method'],
                      url=urlInfo['url'],
                      json=formData,
                      timeout=10,
                      allow_redirects=False)
    return True


def toEmail(message):
    # 第三方 SMTP 服务
    mail_host = "smtp.qq.com"  # 设置服务器
    mail_user = "1024295445@qq.com"  # 用户名
    mail_pass = "pynncwerwnbtbedc"  # 口令

    sender = '1024295445@qq.com'
    receivers = ['1024295445@qq.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

    message = MIMEText(message, 'plain', 'utf-8')
    message['From'] = Header("系统", 'utf-8')
    message['To'] = Header("曲振富", 'utf-8')

    subject = '抢票通知！！！'
    message['Subject'] = Header(subject, 'utf-8')

    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        Log.v("邮件发送成功")
    except smtplib.SMTPException:
        Log.v("无法发送邮件")


def main():
    # toDingding("ss");
    toEmail("ss");


if __name__ == '__main__':
    main()
