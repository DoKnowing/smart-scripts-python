# /!bin/bash
# coding=utf-8
# __author__:smart (737082820@qq.com)
import sys

sys.path.insert(0, '.')
reload(sys)
sys.setdefaultencoding('utf-8')
import logging.config

logging.config.fileConfig('../sources/logging.conf')
logger_name = 'crawlertime'
LOG = logging.getLogger(logger_name)

import cookielib
import urllib2
import random
import requests
import time
from MysqlUtil import *

# 伪装为浏览器
USER_AGENTS = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
]
#
# HEADER = {
#     'User-Agent': random.choice(USER_AGENTS),
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#     'Accept-Language': 'en-US,en;q=0.5',
#     'Connection': 'keep-alive',
#     'Accept-Encoding': 'gzip, deflate',
#     'Content-Type': 'text/html;charset=UTF-8'
# }
HEADER = {
    'User-Agent': random.choice(USER_AGENTS)
}

# 代理IP
HOSTS = ['223.241.118.113:8010', '223.240.209.148:18118', '114.228.75.182:6666',
         '115.221.112.139:25189', '61.143.23.225:47692', "sp.datatub.com:7629"]

# 测试URL
# TEST_URL = 'http://www.xicidaili.com/nn/2333'
TEST_URL = 'https://www.lagou.com'


def open_html(url, proxy=None, cookies_flag=False):
    """
    伪装浏览器打开网页
    :param url:
    :param proxy: 是否添加代理
    :return:
    """
    proxy = None
    # 添加代理
    if proxy:
        proxy = urllib2.ProxyHandler(proxy)
        # proxy = urllib2.ProxyHandler({'http': HOSTS[int(random.random() * len(HOSTS))]})
        proxy_opener = urllib2.build_opener(proxy)
        urllib2.install_opener(proxy_opener)
    # 添加Cookies
    cookies = dict({})
    if cookies_flag:
        cookies = cookielib.CookieJar()
        cookie_support = urllib2.HTTPCookieProcessor(cookies)
        cookies_opener = urllib2.build_opener(cookie_support)
        urllib2.install_opener(cookies_opener)

    # Requst
    request = urllib2.Request(url=url, headers=HEADER)

    html = urllib2.urlopen(request).read()
    return html


def download_html(url, local_path):
    """
    下载html到本地目录
    :param url:  url
    :param local_path:  本地目录
    :return:
    """
    html = open_html(url)
    file = open(local_path, 'w')
    file.write(html)
    file.flush()
    file.close()


def verify_proxy(proxy):
    '''
    :param proxy: ip字典
    :return:
    '''
    flag = False
    ip = proxy['ip']
    port = proxy['port']
    http_type = proxy['http_type']

    proxies = {'%s' % http_type.lower(): '%s://%s:%s' % (http_type.lower(), ip, port)}
    start = time.time()
    try:
        res = requests.get(url=TEST_URL, headers=HEADER, timeout=15, proxies=proxies)
        if not res.ok:
            LOG.error('FAIL Proxy!  ip=%s' % ip)
            proxy = None
        else:
            speed = round(time.time() - start, 2)
            LOG.info('SUCCESS IP=%s:%s [%s] ,speed=%s' % (ip, port, http_type, speed))
            proxy['speed'] = speed
            flag = True
    except Exception, e:
        LOG.error('FAIL Proxy!  ip=%s' % ip)
        proxy = None
    return flag, proxy


def select_proxy_ip():
    sql = "select ip,port,http_type,id from t_smart_proxy_ip where state=-1"
    # sql = "select ip,port,http_type,id from t_smart_proxy_ip limit 500"

    proxies = MysqlUtil.query(sql)
    for p in proxies:
        proxy = dict({})
        proxy.setdefault('ip', p[0])
        proxy.setdefault('port', p[1])
        proxy.setdefault('http_type', p[2])
        flag, px = verify_proxy(proxy)
        update_sql = None
        if flag:
            update_sql = "update t_smart_proxy_ip set state=%d,speed=%f where id=%d" % (1, px['speed'], p[3])
        else:
            update_sql = "update t_smart_proxy_ip set state=%d where id=%d" % (-1, p[3])
        LOG.info('[UPDATE] ip=%s ,sql=%s, Result=%d' % (p[0], update_sql, MysqlUtil.execute(update_sql)))


def disable_ip(ip):
    sql = "update t_smart_proxy_ip set state=%d where ip='%s'" % (-1, ip)
    LOG.info('[Proxy] Disable ip=%s ,Result=%d' % (ip, MysqlUtil.execute(sql)))

    # url = 'http://www.xicidaili.com/nn'
    # print open_html(url, proxy_flag=True, cookies_flag=True)


def get_proxy(limit=1):
    sql = "select ip,port,http_type,id from t_smart_proxy_ip where state=1 limit %d" % limit
    results = MysqlUtil.query(sql)
    proxies = []
    for rs in results:
        proxy = dict({'%s' % rs[2].lower(): '%s://%s:%d' % (rs[2].lower(), rs[0], rs[1])})
        proxies.extend([proxy])
    LOG.debug('[Proxy] %s' % str(proxies))
    return proxies


if __name__ == "__main__":
    select_proxy_ip()
# disable_ip('59.56.224.95')
