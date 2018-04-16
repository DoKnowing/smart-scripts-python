# /!bin/bash
# coding=utf-8
# __author__:smart (737082820@qq.com)

import sys

sys.path.insert(0, '../util')
reload(sys)
sys.setdefaultencoding('utf-8')
import os, time
import logging.config

logging.config.fileConfig('../sources/logging.conf')
logger_name = "root"
LOG = logging.getLogger(logger_name)
from bs4 import BeautifulSoup
import json, string, random
from MysqlUtil import *
from CrawlerUtil import *


def string_format(s):
    if not s:
        return s
    s = string.replace(s, '\n', '')
    s = string.strip(s)
    return s


def format_date(date):
    time = -1
    if u'天' in date:
        time = int(date[0:date.index('天')]) * 24 * 60 * 60
    elif u'小时' in date:
        time = int(date[0:date.index('小时')]) * 60 * 60
    elif u'分钟' in date:
        time = int(date[0:date.index('分钟')]) * 60
    return time


def insert_url(data):
    sql = "insert into t_smart_proxy_ip (ip,port,city,country,anonymity,http_type,speed,connection_time,live_date,verify_date,state) " \
          "values ('%s',%d,'%s','%s','%s','%s',%.3f,%.3f,%d,'%s',%d)" \
          % (data['ip_addr'], data['port'], data['server_city'], data['country'], data['anonymity']
             , data['type'], data['speed'], data['connection_date'], data['live_date'], data['verify_date'], 0)
    LOG.info('[SQL] INSERT SQL = ' + sql)
    LOG.info('[SQL] 插入数据成功? %d' % MysqlUtil.insert(sql))


def get_ip(proxy):
    for http in proxy:
        url = proxy[http]
        return url[url.index('://') + 3:url.rindex(':')]


SLEEP = [2, 5, 10]


class ProxyIPCrawler(object):
    def analysis(self, url, proxy=None):
        # bs = BeautifulSoup( open_html(url), 'html.parser')
        bs = BeautifulSoup(open_html(url, proxy=proxy, cookies_flag=True), 'html5lib')

        page_num = bs.find('div', 'pagination')
        current_num = int(page_num.find('em', 'current').text)
        num_href = page_num.find_all('a')
        total_num = current_num
        for a in num_href:
            try:
                num = int(a.text)
                if total_num < num:
                    total_num = num
            except Exception, e:
                pass
        LOG.info('current_num=%d ,total_num=%d' % (current_num, total_num))
        ips = []
        ip_list = bs.find('table').find_all('tr')
        for ip in ip_list:
            var = ip.find_all('td')
            if not var:
                continue
            d = ({})
            country = string_format(var[0].text)
            if not country:
                country = u'中国'
            d.setdefault('country', country)
            d.setdefault('ip_addr', string_format(var[1].text))
            d.setdefault('port', int(string_format(var[2].text)))
            d.setdefault('server_city', string_format(var[3].text))
            d.setdefault('anonymity', string_format(var[4].text))
            d.setdefault('type', string_format(var[5].text))
            speed = var[6].find('div')['title']
            d.setdefault('speed', float(speed[0:len(speed) - 1]))
            conn_date = var[7].find('div')['title']
            d.setdefault('connection_date', float(conn_date[0:len(conn_date) - 1]))
            d.setdefault('live_date', int(format_date(string_format(var[8].text))))
            d.setdefault('verify_date', '20' + string_format(var[9].text))
            ips.extend([d])
        return current_num, total_num, ips

    def download(self, url, start_page=0, end_page=sys.maxint):
        new_url = url
        if start_page > 0:
            new_url = url + '/' + str(start_page)
        LOG.info('[URL] %s' % new_url)

        proxies = get_proxy(50)
        proxy = random.choice(proxies)

        current_num = -1
        total_num = -1
        flag = True
        while flag:
            try:
                current_num, total_num, datas = self.analysis(new_url, proxy=proxy)
                for data in datas:
                    insert_url(data)
                flag = False
                time.sleep(random.choice(SLEEP))
            except Exception, e:
                try:
                    disable_ip(get_ip(proxy))
                    proxies.remove(proxy)
                    proxy = random.choice(proxies)
                except Exception, e2:
                    proxies = get_proxy(50)
                    proxy = random.choice(proxies)
        while current_num < total_num and current_num < end_page:
            retry = 1
            next_num = current_num + 1
            next_url = url + '/' + str(next_num)
            LOG.info('[URL] %s' % next_url)
            while retry >= 0:
                try:
                    current_num, total_num, datas = self.analysis(next_url, proxy=proxy)
                    for data in datas:
                        insert_url(data)
                    current_num = next_num
                    flag = False
                    time.sleep(random.choice(SLEEP))
                    break
                except Exception, e:
                    try:
                        retry -= 1
                        disable_ip(get_ip(proxy))
                        proxies.remove(proxy)
                        proxy = random.choice(proxies)
                    except Exception, e2:
                        proxies = get_proxy(50)
                        proxy = random.choice(proxies)
            if not proxy:
                LOG.warn('代理已全部使用')


if __name__ == "__main__":
    # 从251开始
    url = 'http://www.xicidaili.com/nn'
    ## 国内HTTPS代理_ip
    # url = 'http://www.xicidaili.com/wn'
    local_path = 'D:/tmp/data/proxy_ip/ip.txt'
    # download_html(url, local_path)
    # print open_html(url)
    proxy = ProxyIPCrawler()
    proxy.download(url, start_page=520)
    # proxy.download(url)
