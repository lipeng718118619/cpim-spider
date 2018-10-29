# -*-*-
# 感谢骚男 『zh (QQ: 315393472)』 提供的源代码
# -*-*-

# ! -*- encoding:utf-8 -*-

import requests

# 要访问的目标页面
targetUrl = "http://test.abuyun.com"
# targetUrl = "http://proxy.abuyun.com/switch-ip"
# targetUrl = "http://proxy.abuyun.com/current-ip"

# 代理服务器
proxyHost = "http-pro.abuyun.com"
proxyPort = "9010"

# 代理隧道验证信息
proxyUser = "H24P4B263QUW8IAP"
proxyPass = "A7AB7830179C99A3"

proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
    "host": proxyHost,
    "port": proxyPort,
    "user": proxyUser,
    "pass": proxyPass,
}

proxies = {
    "http": proxyMeta,
    "https": proxyMeta,
}

resp = requests.get(targetUrl, proxies=proxies)

print
resp.status_code
print
resp.text
