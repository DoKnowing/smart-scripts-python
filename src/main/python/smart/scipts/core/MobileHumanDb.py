# /!bin/bash
# coding=utf-8
# __author__:smart (mashuai@hudongpai.com)
'''
将所有的字段打平,对于每个uid都有唯一的字段
'''


# 标签类型
class Label:
    def __init__(self, category, classify=None, tag=None):
        self.category = category
        self.classify = classify
        self.tag = tag

    def __eq__(self, other):
        if not other:
            return False

        flag = [False, False, False]
        if (not self.category and not other.category) \
                or (self.category == other.category):
            flag[0] = True
        if (not self.classify and not other.classify) \
                or (self.classify == other.classify):
            flag[1] = True
        if (not self.tag and not other.tag) \
                or (self.tag == other.tag):
            flag[2] = True
        for b in flag:
            if not b:
                return b
        return True

    def map(self):
        d = dict({})
        if self.category:
            d.setdefault('category', self.category)
        if self.classify:
            d.setdefault('classify', self.classify)
        if self.tag:
            d.setdefault('tag', self.tag)
        if d:
            return d
        return None


import csv, os, sys

import json
import logging
from elasticsearch import Elasticsearch, NotFoundError

### LOG
logger = logging.getLogger('Label')
formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(module)s.%(lineno)s] : %(message)s')
# 控制台日志
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter  # 也可以直接给formatter赋值
# 为logger添加的日志处理器
logger.addHandler(console_handler)
# 指定日志的最低输出级别，默认为WARN级别
logger.setLevel(logging.INFO)

DIR = u'C:\\Users\\SMA\\Desktop\\数据中心\\tmp\\uid_applabels'

FILE = DIR + '\\' + 'uid_applabels_201703_wastons.csv'
# 根据uidqu去重,不同的label进行合并
UIDS = dict({})

CATEGOTY_TYPE = [u'自然属性', u'社会属性', u'地理位置', u'触达设备']


def add_label(uid, label):
    if not label:
        return
    category = label.category
    classify = label.classify
    # 不包含该uid
    if uid not in UIDS:
        u = dict({})
        u.setdefault('uid', uid)
        u.setdefault('tag', [label])
        UIDS.setdefault(uid, u)
    else:
        tag_list = UIDS.get(uid).get('tag')
        if category in CATEGOTY_TYPE:
            for l in tag_list:
                if not classify and l.classify == classify:
                    return
        else:
            for l in tag_list:
                if l == label:
                    return
        tag_list.extend([label])


def hand_behavivor(csv_reader):
    if not csv_reader:
        return
    header = []
    category = u'网络行为'
    classify = u'行为偏好'
    for row in csv_reader:
        if not header:
            header = row
            # logger.info('[Header] ', str(header))
            print str(header)
        else:
            if row[1]:
                add_label(row[0], Label(category, classify, tag=row[1]))


def hand_ccead_basic(csv_reader):
    if not csv_reader:
        return
    header = []
    for row in csv_reader:
        if not header:
            header = row
            # logger.info('[Header] ', header)
            print str(header)
        else:
            if row[1]:
                add_label(row[0], Label(u'自然属性', u'年龄段', row[1]))
            if row[2]:
                add_label(row[0], Label(u'自然属性', u'性别', row[2]))
            if row[3]:
                add_label(row[0], Label(u'社会属性', u'家庭状况', row[3]))
            if row[4]:
                add_label(row[0], Label(u'社会属性', u'婚姻状况', row[4]))
            if row[5]:
                add_label(row[0], Label(u'地理位置', u'城市', row[5]))


def hand_ccead_deep(csv_reader):
    """
    hand_midea,hand_deep
    :param csv_reader:
    :return:
    """
    if not csv_reader:
        return
    header = []
    for row in csv_reader:
        if not header:
            header = row
            # logger.info('[Header] ', str(header))
            print header
        else:
            if row[1] and row[2] and row[3]:
                add_label(row[0], Label(row[1], row[2], row[3]))
            elif row[1] and row[2]:
                add_label(row[0], Label(row[1], row[2]))
            elif row[1]:
                add_label(row[0], Label(row[1]))


def hand_wastons(csv_reader):
    if not csv_reader:
        return
    header = []
    for row in csv_reader:
        if not header:
            header = row
            # logger.info('[Header] ', str(header))
            print str(header)

        else:
            if row[1]:
                add_label(row[0], Label(u'自然属性', u'性别', row[1]))
            if row[2]:
                add_label(row[0], Label(u'自然属性', u'年龄段', row[2]))
            if row[3]:
                add_label(row[0], Label(u'地理位置', u'城市', row[3]))


def hand_basic(csv_reader):
    if not csv_reader:
        return
    header = []
    for row in csv_reader:
        if not header:
            header = row
            # logger.info('[Header] ', str(header))
            print str(header)

        else:
            if row[1]:
                add_label(row[0], Label(u'地理位置', u'城市等级', row[1]))
            if row[2]:
                add_label(row[0], Label(u'地理位置', u'城市', row[2]))
            if row[3]:
                add_label(row[0], Label(u'自然属性', u'性别', row[3]))
            if row[4]:
                add_label(row[0], Label(u'自然属性', u'年龄段', row[4]))
            if row[5]:
                add_label(row[0], Label(u'社会属性', u'家庭状况', row[5]))
            if row[6]:
                add_label(row[0], Label(u'社会属性', u'婚姻状况', row[6]))
            if row[7]:
                add_label(row[0], Label(u'社会属性', u'线上消费能力', row[7]))
            if row[8]:
                add_label(row[0], Label(u'社会属性', u'线上消费意愿', row[8]))
            if row[9]:
                add_label(row[0], Label(u'触达设备', u'终端品牌', row[9]))
            if row[10]:
                add_label(row[0], Label(u'触达设备', u'终端机型', row[10]))
            if row[11]:
                add_label(row[0], Label(u'触达设备', u'终端价位', row[11]))


def hand_tags(csv_reader):
    if not csv_reader:
        return
    header = []
    for row in csv_reader:
        if not header:
            header = row
            # logger.info('[Header] ', str(header))
            print str(header)

        else:
            if row[1]:
                add_label(row[0], Label(u'自然属性', u'性别', row[1]))
            if row[2]:
                add_label(row[0], Label(u'自然属性', u'年龄', row[2]))
            if row[3]:
                add_label(row[0], Label(u'自然属性', u'年龄段', row[3]))
            if row[4]:
                add_label(row[0], Label(u'社会属性', u'线上消费能力', row[4]))
            if row[5]:
                add_label(row[0], Label(u'社会属性', u'线上消费意愿', row[5]))
            if row[6]:
                add_label(row[0], Label(u'社会属性', u'家庭状况', row[6]))
            if row[7]:
                add_label(row[0], Label(u'社会属性', u'婚姻状况', row[7]))

            if row[8] and row[9] and row[10]:
                add_label(row[0], Label(row[8], row[9], row[10]))
            elif row[8] and row[9]:
                add_label(row[0], Label(row[8], row[9]))
            elif row[8]:
                add_label(row[0], Label(row[8]))


def uid_print():
    if not UIDS:
        return
    for uid in UIDS:
        tag_list = UIDS[uid].get('tag')
        d = dict({})
        d.setdefault('uid', uid)
        d.setdefault('id', uid)
        label_list = []
        for label in tag_list:
            label_list.extend([label.map()])
        d.setdefault('tag', label_list)
        print json.dumps(d, lambda o: o.__dict__)


HOSTS = ['dev4:9205', 'dev5:9205', 'dev6:9205']
HOSTS_STR = 'dev4:9205,dev5:9205,dev6:9205'
SRC_INDEX_NAME = 'ds-hermes-mobile-test'
SRC_INDEX_TYPE = 'weibo'
es = Elasticsearch(hosts=['dev4:9205', 'dev5:9205', 'dev6:9205'])

# es.indices.create(index=SRC_INDEX_NAME)
MAPPINGS = '{"weibo":{"dynamic_templates":[{"strings":{"mapping":{"index":"not_analyzed","doc_values":true,' \
           '"type":"string"},"match":"*","match_mapping_type":"string"}}],"_all":{"enabled":false},"properties":{' \
           '"tag":{"type":"nested","properties":{"category":{"type":"string","index":"not_analyzed",' \
           '"doc_values":true},"classify":{"type":"string","index":"not_analyzed","doc_values":true},' \
           '"tag":{"type":"string","index":"not_analyzed","doc_values":true}}}}}} '


# es.indices.put_mapping(index=SRC_INDEX_NAME, doc_type=SRC_INDEX_TYPE, body=MAPPINGS)
def mapping_create(index, type, mappings):
    if es.indices.exists(index=index):
        try:
            if es.indices.get_mapping(index=index, doc_type=type):
                logger.info('[Mapping] index=' + index + ' ,type=' + type + u'已存在')
                return True
        except NotFoundError:
            flag = es.indices.put_mapping(index=index, doc_type=type, body=mappings)
            if 'acknowledged' in flag:
                logger.info('[Mapping] create ' + index + '/' + type + ' Mapping : ' + str(flag['acknowledged']))
                return flag['acknowledged']
            else:
                raise Exception(flag)
        except Exception, e:
            raise e.message
    else:
        if es.indices.create(index=index):
            flag = es.indices.put_mapping(index=index, doc_type=type, body=mappings)
            if 'acknowledged' in flag:
                logger.info('[Mapping] create ' + index + '/' + type + ' Mapping : ' + str(flag['acknowledged']))
                return flag['acknowledged']
            else:
                raise Exception(flag)
        else:
            raise Exception(u'create ' + index + '/' + type + ' Failed')


def writer():
    if not UIDS:
        return
    if not mapping_create(SRC_INDEX_NAME, SRC_INDEX_TYPE, MAPPINGS):
        return

    for uid in UIDS:
        tag_list = UIDS[uid].get('tag')
        d = dict({})
        d.setdefault('uid', uid)
        d.setdefault('id', uid)
        label_list = []
        for label in tag_list:
            label_list.extend([label.map()])
        d.setdefault('tag', label_list)
        # print json.dumps(d, lambda o: o.__dict__)
        es_writer(SRC_INDEX_NAME, SRC_INDEX_TYPE, json.dumps(d, lambda o: o.__dict__), uid)


def es_writer(index, type, source, id=None):
    print es.index(index=index, doc_type=type, body=source, id=id)


if __name__ == "__main__":
    files = os.listdir(DIR)

    for file in files:
        if not os.path.isfile(DIR + '/' + file):
            continue
        csv_reader = csv.reader(open(DIR + '/' + file, 'r'))

        if file.__contains__('behavior'):
            hand_behavivor(csv_reader)
        elif file.__contains__('ccead_basic'):
            hand_ccead_basic(csv_reader)
            # pass
        elif file.__contains__('ccead_deep'):
            hand_ccead_deep(csv_reader)
            # pass
        elif file.__contains__('wastons'):
            hand_wastons(csv_reader)
            # pass
        elif file.__contains__('basic'):
            hand_basic(csv_reader)
            # pass
        elif file.__contains__('deep'):
            hand_ccead_deep(csv_reader)
        elif file.__contains__('midea'):
            hand_ccead_deep(csv_reader)
            # pass
        elif file.__contains__('tags'):
            hand_tags(csv_reader)
            # pass
    # uid_print()

    writer()
