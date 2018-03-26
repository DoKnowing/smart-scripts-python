# /!bin/bash
# coding=utf-8
# __author__:smart (737082820@qq.com)

import sys, os

sys.path.insert(0, '..')

import logging.config

logging.config.fileConfig('../../../../../sources/logging.conf')
logger_name = "root"
LOG = logging.getLogger(logger_name)

import time
import urllib2
from Crawler import *


class LagouCrawler(Crawler):
    def download(self, url, dir, file_name=None):
        if not file_name:
            file_name = str(int(round(time.time() * 1000))) + '.html'
        html = urllib2.urlopen(url).read()
        file = open(dir + '/' + file_name, 'w')
        file.write(html)
        file.close()

    def analysis(self, path):
        print path


if __name__ == "__main__":
    dir = 'D:/tmp/data/lagou/html'
    url = 'https://www.lagou.com/'
    if not os.path.exists(dir) or not os.path.isdir(dir):
        LOG.info(u'dir=' + dir + u'不存在,创建该目录: ')
        os.makedirs(dir)
    LOG.info(u'开始获取拉钩网页面数据')
    lagou = LagouCrawler()
    lagou.download(url=url, dir=dir)
