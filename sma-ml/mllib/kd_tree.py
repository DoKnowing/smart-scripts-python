# -*- coding: utf-8 -*-
# __author__:smart (737082820@qq.com)

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

from operator import itemgetter

'''
二维的kd-tree
'''

point_list = [(2, 3), (5, 4), (9, 6), (4, 7), (8, 1), (7, 2)]

point_list.sort(key=itemgetter(1), reverse=True)

print point_list
