# /!bin/bash
# coding=utf-8
# __author__:smart (mashuai@hudongpai.com)
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkContext
from operator import *
import json, re
import matplotlib.pyplot as plt

CITY_REG = u'(.*)市'


def city_format(city):
    if not city:
        return u'未知'
    if re.match(CITY_REG, city):
        return city.replace(u'市', '')
    return city


def show_y(rects):
    """
    显示柱状图的数值
    :param rects:
    :return:
    """
    for rect in rects:
        height = rect.get_height()
        plt.text(rect.get_x() + rect.get_width() / 2. - 0.1, 1.02 * height, '%d' % int(height))


def figure_histogram(data, fig_name=None):
    x_axis = []
    y_axis = []
    for r in data:
        x_axis.append(r[0])
        y_axis.append(r[1])

    fig = plt.gcf()
    fig.set_size_inches(16, 10)

    p = plt.bar(x_axis, y_axis, color="green")
    # 显示数值
    show_y(p)
    if fig_name:
        fig.savefig(u'D:/tmp/data/lagou/html/' + fig_name, dpi=100)
    plt.show()


def figure_line_chart(data, fig_name=None):
    x_axis = []
    y_axis = []
    for r in data:
        x_axis.append(r[0])
        y_axis.append(float('%.2f' % r[1]))

    fig = plt.figure()
    fig.set_size_inches(16, 10)

    plt.plot(x_axis, y_axis)
    # 设置数值
    for a, b in zip(x_axis, y_axis):
        plt.text(a, b, b, ha='center', va='bottom', fontsize=12)

    if fig_name:
        fig.savefig(u'D:/tmp/data/lagou/html/' + fig_name, dpi=100)
    plt.show()


def lagou_data(json_data):
    d = json.loads(json_data, encoding='utf-8')
    lt = []
    lt.extend([d['id']])
    lt.extend([d['job_name'].lower()])
    lt.extend([d['city']])
    lt.extend([d['saraly_start']])
    lt.extend([d['saraly_end']])
    lt.extend([d['develop']])
    lt.extend([d['publish_time']])
    return lt


def analysis_lagou():
    data_path = 'D:/tmp/data/lagou/html/data_java.txt'
    sc = SparkContext(master='local[3]', appName='History')
    data = sc.textFile(data_path, 1).map(lambda o: lagou_data(o))
    # 统计jobName
    result = data.map(lambda o: (o[1], 1)).reduceByKey(add).sortBy(lambda o: o[1], ascending=False).collect()
    figure_histogram(result, u'职位分布图.jpg')


def saraly():
    data_path = 'D:/tmp/data/lagou/html/data_java.txt'
    sc = SparkContext(master='local[3]', appName='History')
    data = sc.textFile(data_path, 1).map(lambda o: lagou_data(o))
    data.cache()

    # 薪资与城市的区别
    result = data.map(lambda o: (city_format(o[2]), o[3])).reduceByKey(lambda a, b: (a + b) / 2.0). \
        filter(lambda o: o[0]).sortByKey(ascending=True).collect()
    print result
    figure_line_chart(result, u'城市最低工资分布.jpg')

    result2 = data.map(lambda o: (city_format(o[2]), o[4])).reduceByKey(lambda a, b: float((a + b) / 2.0)). \
        filter(lambda o: o[0]).sortByKey(ascending=True).collect()
    print result2
    figure_line_chart(result2, u'城市最高工资分布.jpg')


if __name__ == "__main__":
    # analysis_lagou()
    saraly()
    # data = [(u'北京', 2.5), (u'合肥', 3333333),(u'天津', 12222222.03)]
    # figure_line_chart(data)
