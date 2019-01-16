# -*- coding: utf-8 -*-
# __author__:smart (737082820@qq.com)

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

import random

import matplotlib
import matplotlib.pyplot as plt


def init_data():
    """
    初始化数据
    初始化 1000个点,范围在 (0,0) - (500,100)

    :return:
    """
    data_file = open("D:\\data\\ml\\kmeans.txt", 'w')
    data = []
    for i in range(0, 500):
        x = 0
        y = 0
        # if i % 3 == 0:
        #     x = random.randint(0, 100)
        #     y = random.randint(0, 200)
        # elif i % 3 == 1:
        #     x = random.randint(200, 300)
        #     y = random.randint(100, 200)
        # else:
        #     x = random.randint(350, 500)
        #     y = random.randint(300, 350)
        x = random.randint(0, 5000)
        y = random.randint(0, 5000)
        data_file.write(str(x) + ',' + str(y))
        data_file.write('\n')
        data.append((x, y))
    data_file.flush()
    data_file.close()
    return data


def plot(data, center):
    data_xy = dict()

    for key in data.keys():
        vs = data[key]
        x = []
        y = []
        for v in vs:
            x.append(v[0])
            y.append(v[1])
        data_xy[key] = (x, y)
    center_x = []
    center_y = []
    for ct in center:
        center_x.append(ct[1][0])
        center_y.append(ct[1][1])

    f1 = plt.figure(1)
    for key in data_xy.keys():
        if key == 0:
            plt.scatter(data_xy[0][0], data_xy[0][1], marker='x', color='m', label='1')
        elif key == 1:
            plt.scatter(data_xy[1][0], data_xy[1][1], marker='o', color='c', label='2')
        else:
            plt.scatter(data_xy[2][0], data_xy[2][1], marker='+', color='g', label='3')
    plt.scatter(center_x, center_y, marker='*', color='r', label='4')
    # plt.savefig()
    plt.show()


def data_read():
    data_file = open("D:\\data\\ml\\kmeans.txt", 'r')
    data = []
    for line in data_file:
        str = line.strip().split(",")
        data.append((int(str[0]), int(str[1])))

    return data


def train(data, num_clusters=3, maxIterations=100):
    """
    :param data: [(x,y),...]
    :param num_clusters: K类
    :param maxIterations:  结束标志
    :return: 
    """
    # 1. 随机挑选K的样本作为中心点
    if num_clusters < 1:
        raise Exception("num_clusters cannot less than 1")
    last_center = []
    for i in range(0, num_clusters):
        tmp = data[random.randint(0, len(data) - 1)]
        last_center.append((i, tmp))
    print "init center are : ", last_center
    data_calss = []
    center = last_center
    it = 0
    while it <= maxIterations:
        it += 1
        center, data_calss = calc_center(center, data)
        # print "iteration: ", it, " ,center : ", center

        ct_int = 0
        if len(center) != len(last_center):
            print 'init cluster K is bad'
            return last_center
        for i in range(0, len(last_center)):
            ct_int += calc_instance(last_center[i][1], center[i][1])
        if ct_int < 10:
            break
        last_center = center
    print "end center : ", center
    plot(data=data_calss, center=center)
    return center


def calc_center(centers, datas):
    data = dict()
    for d in datas:
        minCenter = centers[0]
        for center in centers:
            if calc_instance(d, minCenter[1]) > calc_instance(d, center[1]):
                minCenter = center

        if minCenter[0] in data:
            data.get(minCenter[0]).append(d)
        else:
            data[minCenter[0]] = [d]

    ct2 = []
    for key in data.keys():
        dt = data[key]
        x = 0
        y = 0
        for d in dt:
            x += d[0]
            y += d[1]
        ct2.append((key, (x / len(dt), y / len(dt))))
    return ct2, data


def calc_instance(d1, d2):
    return pow(d1[0] - d2[0], 2) + pow(d1[1] - d2[1], 2)


# c0 = [[], []]
# c1 = [[], []]
# c2 = [[], []]
# for i in range(0, 100):
#     center = train(data=data_read())
#     for c in center:
#         if 0 == c[0]:
#             c0[0].append(c[1][0])
#             c0[1].append(c[1][1])
#         elif 1 == c[0]:
#             c1[0].append(c[1][0])
#             c1[1].append(c[1][1])
#         else:
#             c2[0].append(c[1][0])
#             c2[1].append(c[1][1])
#     print "=============================\n"
# f = plt.figure(0)
# plt.scatter(c0[0], c0[1], marker='+', color='g', label='1')
# plt.scatter(c1[0], c0[1], marker='o', color='m', label='2')
# plt.scatter(c2[0], c0[1], marker='x', color='r', label='3')
# plt.show()
center = train(data=data_read())
# init_data()
