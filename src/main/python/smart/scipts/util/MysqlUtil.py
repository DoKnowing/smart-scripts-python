# /!bin/bash
# coding=utf-8
# __author__:smart (737082820@qq.com)


import sys

reload(sys)
sys.setdefaultencoding('utf-8')

import pymysql
import threading
from configparser import ConfigParser

cf = ConfigParser()
cf.read('../sources/mysql.conf')


class MysqlUtil:
    @staticmethod
    def query(sql):
        return Connection.instance().select(sql)

    @staticmethod
    def insert(sql):
        return Connection.instance().insert(sql)

    @staticmethod
    def execute(sql):
        return Connection.instance().execute(sql)

    @staticmethod
    def close():
        return Connection.instance().close()


class Connection(object):
    _lock = threading.Lock()
    __conn = None
    __cursor = None

    def __init__(self):
        self.__conn = pymysql.connect(host=cf.get('db', 'host'), port=cf.getint('db', 'port'),
                                      user=cf.get('db', 'user'), passwd=cf.get('db', 'passwd'), db=cf.get('db', 'db'),
                                      charset='utf8')
        self.__cursor = self.__conn.cursor()
        pass

    @classmethod
    def instance(cls, *args, **kwargs):
        if not hasattr(Connection, '_instance'):
            with Connection._lock:
                if not hasattr(Connection, '_instance'):
                    Connection._instance = Connection(*args, **kwargs)
        return Connection._instance

    def select(self, sql):
        self.__cursor.execute(sql)
        return self.__cursor.fetchall()

    def insert(self, sql):
        success = self.__cursor.execute(sql)
        self.__conn.commit()
        return success

    def execute(self, sql):
        success = self.__cursor.execute(sql)
        self.__conn.commit()
        return success

    def close(self):
        if self.__cursor:
            self.__cursor.close()
        if self.__conn:
            self.__conn.close()
        Connection._instance = None
