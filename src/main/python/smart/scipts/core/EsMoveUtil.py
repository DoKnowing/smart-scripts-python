# /!bin/bash
# coding=utf-8
# __author__:smart (mashuai@hudongpai.com)

import sys

from elasticsearch import Elasticsearch
from pyspark import SparkContext

'''
使用elasticsearch,spark实现es索引的数据迁移
'''

HOSTS = ['dev4:9205', 'dev5:9205', 'dev6:9205']
SRC_INDEX_NAME = 'ds-hermes-scdb-v1'
SRC_INDEX_TYPE = 'weibo'

DES_INDEX_NAME = 'ds-hermes-20180314'
DEFAULT_MATCH_QUERY = '{"query":{"match_all":{}}}'


# 构建query_json
def build_query():
    return ''


def move():
    sc = SparkContext(master='local[1]', appName='EsMove')
    es_rdd = sc.newAPIHadoopRDD(inputFormatClass='org.elasticsearch.hadoop.mr.EsInputFormat',
                                keyClass='org.apache.hadoop.io.NullWritable',
                                valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable',
                                conf={'es.resource': SRC_INDEX_NAME + '/' + SRC_INDEX_TYPE})

    print es_rdd.filter()


if __name__ == "__main__":
    move()
