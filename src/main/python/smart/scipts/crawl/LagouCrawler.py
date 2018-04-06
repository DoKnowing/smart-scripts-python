# /!bin/bash
# coding=utf-8
# __author__:smart (737082820@qq.com)

import sys

import chardet

sys.path.insert(0, '../util')
reload(sys)
sys.setdefaultencoding('utf-8')
import os
import logging.config

logging.config.fileConfig('../sources/logging.conf')
logger_name = "root"
LOG = logging.getLogger(logger_name)

import time
import urllib2
import json
import re, string
import pymysql
import random
from bs4 import BeautifulSoup
import html5lib
from MysqlUtil import *
from CrawlerUtil import *

CATEGORY_DICT = dict({})

reg = u'(.*):(.*)发布'
reg2 = u'(.*)天前发布'


def insert_url():
    path = 'D:/tmp/data/lagou/html/homepage.txt'
    lines = open(path, 'r')
    for line in lines:
        ccu = json.loads(line)
        sql = "insert into t_smart_crawler_url (site_name,site_type,category,classify,tag,url,state) values ('拉勾网','招聘网','" + \
              ccu['category'] + "','" + ccu['classify'] + "','" + ccu['tag'] + "','" + ccu['url'] + "',1)"
        LOG.info('[SQL] INSERT SQL = ' + sql)
        LOG.info('[SQL] 插入数据成功? %d' % MysqlUtil.insert(sql))


def get_url(category, classify, tag=None):
    sql = None
    if not tag:
        sql = "select category,classify,tag,url from t_smart_crawler_url where state=1 and category='%s' and classify='%s'" % (
            category, classify)
    else:
        sql = "select category,classify,tag,url from t_smart_crawler_url where state=1 and category='%s' and classify='%s' and tag='%s'" % (
            category, classify, tag)
    LOG.info('[SQL] Select SQL = %s' % sql)
    results = MysqlUtil.query(sql)
    result_dict = []
    for rs in results:
        d = dict({})
        d.setdefault('category', rs[0])
        d.setdefault('classify', rs[1])
        d.setdefault('tag', rs[2])
        d.setdefault('url', rs[3])
        result_dict.extend([d])
    return result_dict


def string_format(s):
    if not s:
        return s
    s = string.replace(s, '\n', '')
    s = string.strip(s)
    return s


def get_id(url):
    """
    格式化mid
    :param url:
    :return:
    """
    if not url:
        raise Exception("url can't NULL")
    return url[url.rindex('/') + 1:url.rindex('.')]


def str2Num(str):
    """
    将类似 20K 变为 200000,支持k,w,kw
    :param str:
    :return:
    """
    if not str:
        return -1
    num = -1
    str = str.lower().strip()
    if str.endswith('kw'):
        num_str = str[0:str.index('kw')]
        num = int(num_str) * 10000000
    elif str.endswith('w'):
        num_str = str[0:str.index('w')]
        num = int(num_str) * 10000
    elif str.endswith('k'):
        num_str = str[0:str.index('k')]
        num = int(num_str) * 1000
    return num


def format_time(time_str):
    if not time:
        return None
    if re.match(u'(.*)天前  发布于拉勾网', time_str):
        time_str = time.strftime('%Y-%m-%d',
                                 time.localtime(time.time() - 86400 * int(time_str[0:time_str.rindex('天')])))
    elif re.match(u'(.*):(.*)  发布于拉勾网', time_str):
        time_str = time.strftime('%Y-%m-%d', time.localtime()) + ' ' + time_str[0:5]
    elif re.match(u'(.*)-(.*)-(.*)  发布于拉勾网', time_str):
        time_str = time_str[0, time_str.linde(' ')]
    elif re.match(u'(.*):(.*)发布', time_str):
        time_str = time.strftime('%Y-%m-%d', time.localtime()) + ' ' + time_str[0:5]
    elif re.match(u'(.*)天前发布', time_str):
        time_str = time.strftime('%Y-%m-%d',
                                 time.localtime(time.time() - 86400 * int(time_str[0:time_str.rindex('天')])))
    return time_str


class LagouCrawler(object):
    def getItem(self, url):
        if not url:
            raise Exception(u'url 不能为空')
        return url[url.rindex('/') + 1:url.rindex('.')]

    def hand_homepage(self, local_path):
        """
        处理拉钩网首页
        :return:
        """
        home_page = 'https://www.lagou.com/'
        html = open_html(url)
        bs = BeautifulSoup(html, "html.parser")
        # 种类
        categorys = bs.find_all('div', 'menu_box')
        category_list = []
        for cg in categorys:
            # 一级分类
            category = string.replace(cg.find('h2').text, '\n', '')
            category = string.replace(category, ' ', '')
            classifys = cg.find_all('dl')
            for cf in classifys:
                ## 二级分类
                classify = cf.find('span').text
                tags = cf.find_all('a')
                for tag in tags:
                    ## 三级分类
                    tag_dict = dict({})
                    tag_dict.setdefault('category', category)
                    tag_dict.setdefault('classify', classify)
                    tag_dict.setdefault('tag', tag.text)
                    tag_dict.setdefault('url', tag['href'])
                    category_list.extend([tag_dict])
        file = open(local_path, 'w')
        for l in category_list:
            c_json = json.dumps(l, lambda o: o.__dict__, encoding='UTF-8', ensure_ascii=False)
            LOG.info('[HomePage] %s' % c_json)
            file.write(c_json)
            file.write('\n')
        file.close()

    def hand_job_list(self, url, proxy=None):
        html = open_html(url, proxy)
        chardet.detect(html)
        bs = BeautifulSoup(html, 'html5lib')
        job_list = bs.find_all('div', 'position')
        jobs = []
        for job in job_list:
            jobinfo = dict({})
            p_top = job.find('div', 'p_top').find('a')
            url = p_top['href']
            jobinfo.setdefault('url', url)
            jobName = job.find('h3').text
            jobinfo.setdefault('job_name', jobName)
            location = job.find('span', 'add').text
            location = location[1:len(location) - 1]
            jobinfo.setdefault('job_loc', location)
            publish_time = job.find('span', 'format-time').text
            if re.match(reg, publish_time):
                publish_time = time.strftime('%Y-%m-%d', time.localtime()) + ' ' + publish_time[0:5]
            elif re.match(reg2, publish_time):
                publish_time = time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400 * int(publish_time[0])))
            jobinfo.setdefault('publish_time', publish_time)
            jobs.extend([jobinfo])
            LOG.debug(u'职位名: %s ,工作地点: %s, url: %s ,发布时间: %s' % (jobName, location, url, publish_time))
        # 获取当前页:
        current_num = int(string_format(bs.find('span', 'curNum').text))
        # 总页数
        total_num = int(string_format(bs.find('span', 'span totalNum').text))
        return total_num, current_num, jobs

    def hand_job_info(self, url, proxy=None):
        LOG.info('[JobInfo] URL=%s' % url)
        bs = BeautifulSoup(open_html(url, proxy), 'html5lib')
        job_info = dict({})
        # 1. id
        job_info.setdefault('id', get_id(url))
        # url
        job_info.setdefault('url', url)
        # job_name 职位名称
        job_name = bs.find('div', 'job-name').find('span', 'name').text
        job_info.setdefault('job_name', job_name)

        # job_request
        requests = bs.find('dd', 'job_request').find_all('span')
        ## saraly
        saraly = string_format(string.replace(requests[0].text, '/', ''))
        ss = saraly.split('-')
        job_info.setdefault('saraly_start', str2Num(ss[0]))
        job_info.setdefault('saraly_end', str2Num(ss[1]))
        ## 经验要求
        job_info.setdefault('experience', string_format(string.replace(requests[2].text, '/', '')))
        ## 学历要求
        job_info.setdefault('education', string_format(string.replace(requests[3].text, '/', '')))
        ## 工作类型
        job_info.setdefault('job_category', string_format(string.replace(requests[4].text, '/', '')))

        ## 标签
        job_tag = bs.find('ul', 'position-label clearfix').find_all('li')
        # 转为数组
        job_tag_value = []
        for tag in job_tag:
            job_tag_value.extend([string_format(tag.text)])
            job_info.setdefault('job_tag', job_tag_value)

        ## 职位要求描述
        job_info.setdefault('description', string_format(bs.find('dd', 'job_bt').find('div').text))

        ## 工作地点
        location = string_format(bs.find('div', 'work_addr').text).replace(' ', '').replace('查看地图', '')
        job_info.setdefault('workplace', location)
        job_info.setdefault('city', location.split('-')[0])

        ## 发布者信息[公司信息]
        job_company = bs.find('dl', 'job_company')

        company_name = job_company.find('h2', 'fl').next
        job_info.setdefault('company_name', string_format(company_name))

        c_feature = job_company.find('ul', 'c_feature').find_all('li')

        ## 公司领域
        job_info.setdefault('territory', string_format(c_feature[0].find('i').next))
        ## 公司类型
        job_info.setdefault('develop', string_format(c_feature[1].find('i').next))
        ## 规模
        job_info.setdefault('scale', string_format(c_feature[2].find('i').next))
        ## 公司主页
        job_info.setdefault('lagou_page', string_format(c_feature[3].find('a')['href']))

        ## 发表时间
        job_info.setdefault('publish_time', format_time(bs.find('p', 'publish_time').text))

        LOG.debug(json.dumps(job_info, lambda o: o.__dict__, encoding='utf-8', ensure_ascii=False))
        return job_info

        # 下载当前分类的子分类的列表

    def download_category(self, dir, category, classify, tag=None):
        ## 获取url
        cctus = get_url(category=category, classify=classify, tag=tag)
        ## 获取当前页的所有job列表
        proxies = get_proxy(500)
        proxy = random.choice(proxies)
        retry_cnt = 1
        file = open(dir + '/data_houduankaifa.txt', 'w')
        for cctu in cctus:
            try:
                url = cctu['url']
                total_num, current_num, jobs = self.hand_job_list(url, proxy)
                LOG.info('处理第1页任务列表,共%d页... url=%s' % (total_num, url))

                for job in jobs:
                    retry = retry_cnt
                    while retry >= 0:
                        try:
                            job_info = self.hand_job_info(job['url'], proxy=proxy)
                            job_info.setdefault('category', cctu['category'])
                            job_info.setdefault('classify', cctu['classify'])
                            job_info.setdefault('subject', cctu['tag'])
                            file.write(json.dumps(job_info, lambda o: o.__dict__, encoding='utf-8', ensure_ascii=False))
                            file.write('\n')
                            time.sleep(10)
                            break
                        except Exception, e:
                            LOG.error(e.message)
                            retry -= 1
                            if retry >= 0:
                                LOG.info('抓取失败,重新第%d(%d)次抓取页面 url=%s' % (retry_cnt - retry, retry_cnt, job['url']))
                            time.sleep(30)
                            try:
                                proxies.remove(proxy)
                                proxy = random.choice(proxies)
                            except Exception, e:
                                proxies = get_proxy(500)
                                proxy = random.choice(proxies)
                        finally:
                            file.flush()

                while current_num < total_num:
                    current_num += 1
                    job_url = url + str(current_num)
                    LOG.info('处理第%d页任务列表,共%d页... url=%s' % (current_num, total_num, job_url))
                    try:
                        total_num, current_num, jobs = self.hand_job_list(job_url, proxy=proxy)
                        time.sleep(5)

                        for job in jobs:
                            retry = retry_cnt
                            while retry >= 0:
                                try:
                                    job_info = self.hand_job_info(job['url'], proxy=proxy)
                                    job_info.setdefault('category', cctu['category'])
                                    job_info.setdefault('classify', cctu['classify'])
                                    job_info.setdefault('subject', cctu['tag'])
                                    file.write(
                                        json.dumps(job_info, lambda o: o.__dict__, encoding='utf-8',
                                                   ensure_ascii=False))
                                    file.write('\n')
                                    time.sleep(10)
                                    break
                                except Exception, e:
                                    LOG.error(e.message)
                                    retry -= 1
                                    if retry >= 0:
                                        LOG.info(
                                            '抓取失败,重新第%d(%d)次抓取页面 url=%s' % (retry_cnt - retry, retry_cnt, job['url']))
                                    time.sleep(30)
                                    try:
                                        proxies.remove(proxy)
                                        proxy = random.choice(proxies)
                                    except Exception, e:
                                        proxies = get_proxy(500)
                                        proxy = random.choice(proxies)
                                finally:
                                    file.flush()
                    except Exception, e:
                        LOG.error(e)
                        current_num -= 1
                        time.sleep(10)

            except Exception, e:
                LOG.error(e)
                time.sleep(60)
        file.close()


if __name__ == "__main__":
    dir = 'D:/tmp/data/lagou/html'
    url = 'https://www.lagou.com/'
    # if not os.path.exists(dir) or not os.path.isdir(dir):
    #     LOG.info(u'dir=' + dir + u'不存在,创建该目录: ')
    #     os.makedirs(dir)
    LOG.info(u'开始获取拉钩网页面数据')
    lagou = LagouCrawler()
    # lagou.download(url=url, dir=dir)
    # 处理首页
    LOG.info(u'处理首页数据')
    # lagou.hand_homepage(dir + '/homepage.txt')
    # lagou.download(url='https://www.lagou.com/zhaopin/Java/', dir=dir, file_name='java.html')
    # total_num, current_num, jobs = lagou.hand_job_list(dir + '/java.html')
    # LOG.info('[Page] current_num: %s ,total_num: %s' % (current_num, total_num))
    # LOG.info('[Jobs] jobs : %s' % json.dumps(jobs, lambda o: o.__dict__))
    # lagou.getItem('https://www.lagou.com/jobs/4085676.html')
    # lagou.download(url='https://www.lagou.com/zhaopin/Java/2', dir=dir, file_name='java2.html')
    # lagou.hand_job_info(dir + '/4085676.html')
    # insert_url()
    # lagou.download_category(dir, category=u'技术', classify=u'高端职位', tag=U'CTO')
    # lagou.download_category(dir, category=u'技术', classify=u'后端开发', tag=U'Java')
    lagou.download_category(dir, category=u'技术', classify=u'后端开发')
