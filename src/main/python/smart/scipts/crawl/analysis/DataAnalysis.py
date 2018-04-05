# /!bin/bash
# coding=utf-8
# __author__:smart (mashuai@hudongpai.com)
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkContext
from operator import *
import json
import matplotlib.pyplot as plt


def show_y(rects):
    """
    显示柱状图的数值
    :param rects:
    :return:
    """
    for rect in rects:
        height = rect.get_height()
        plt.text(rect.get_x() + rect.get_width() / 2. - 0.1, 1.02 * height, '%d' % int(height))


def lagou_data(json_data):
    d = json.loads(json_data, encoding='utf-8')
    lt = []
    lt.extend([d['id']])
    lt.extend([d['job_name'].lower()])
    lt.extend([d['city']])
    lt.extend([d['saraly_start']])
    lt.extend([d['develop']])
    lt.extend([d['publish_time']])

    return lt


def analysis_lagou():
    data_path = 'D:/tmp/data/lagou/html/data_java.txt'
    sc = SparkContext(master='local[3]', appName='History')
    data = sc.textFile(data_path, 1).map(lambda o: lagou_data(o))
    # 统计jobName
    result = data.map(lambda o: (o[1], 1)).reduceByKey(add).sortBy(lambda o: o[1], ascending=False).collect()
    x_axis = []
    y_axis = []
    for r in result:
        x_axis.append(r[0])
        y_axis.append(r[1])

    fig = plt.gcf()
    fig.set_size_inches(16, 10)

    p = plt.bar(x_axis, y_axis, color="green")
    # 显示数值
    show_y(p)

    fig.savefig(u'D:/tmp/data/lagou/html/职位分布图.jpg', dpi=100)
    plt.show()


if __name__ == "__main__":
    analysis_lagou()
