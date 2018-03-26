# /!bin/bash
# coding=utf-8
# __author__:smart (mashuai@hudongpai.com)

import sys, os

from elasticsearch import Elasticsearch
from pyspark import SparkContext, SparkConf

'''
使用elasticsearch,spark实现es索引的数据迁移
'''

HOSTS = ['dev4:9205', 'dev5:9205', 'dev6:9205']
HOSTS_STR = 'dev4:9205,dev5:9205,dev6:9205'
# SRC_INDEX_NAME = 'ds-hermes-scdb-v1'
SRC_INDEX_NAME = 'ds-hermes-mobile-v1'
SRC_INDEX_TYPE = 'weibo'

DES_INDEX_NAME = 'ds-hermes-20180314'
DEFAULT_MATCH_QUERY = '{"query":{"match_all":{}}}'

SPARK_CLASSPATH = 'D:/hadoop/elasticsearch-hadoop-5.5.3/dist/elasticsearch-hadoop-5.5.3.jar'
os.environ['SPARK_CLASSPATH'] = SPARK_CLASSPATH


# 构建query_json
def build_query():
    return ''


def move():
    # conf = SparkConf().setMaster('local[1]').setAppName('EsMove')
    # sc = SparkContext(conf=conf)
    sc = SparkContext(master='local[3]', appName='EsMove')
    es_rdd = sc.newAPIHadoopRDD(inputFormatClass='org.elasticsearch.hadoop.mr.EsInputFormat',
                                keyClass='org.apache.hadoop.io.NullWritable',
                                valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable',
                                conf={'es.resource': SRC_INDEX_NAME + '/' + SRC_INDEX_TYPE, 'es.nodes': HOSTS_STR})
    # print es_rdd.count()

    # print es_rdd.first()[1]['msg_type']
    print es_rdd.first()


if __name__ == "__main__":
    move()
