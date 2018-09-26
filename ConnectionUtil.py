#!/usr/bin/env python
# coding: utf-8
# @Time    : 2018/8/15 17:52
# @Author  : zeng yu 
# @Site    : 
# @File    : ConnectionUtil.py
# @Software: PyCharm

import happybase


class HbaseConnect(object):
    def __init__(self, host="localhost", port=19090):
        self.host = host
        self.port = port

        pass

    def get_hbase_connect_pool(self):
        """
        :return: 获取happybase的连接池
        """
        pool = happybase.ConnectionPool(200, host=self.host, port=self.port)
        return pool


class MysqlConnect(object):
    def __init__(self, host="localhost", port=3306, user=None, password=None, db=None):
        """
        :param host:地址
        :param port:端口号
        :param user:用户名
        :param password:密码
        :param db:数据库名
        """
        self.conn = MySQLdb.connect(
            host=host,
            port=port,
            user=user,
            passwd=password,
            db=db,
            charset='utf8',
        )
        self.cursor = self.conn.cursor()

    def execute_sql(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            print '插入数据失败!'
            print(e.message)
            print(sql)
            self.conn.rollback()


if __name__ == '__main__':
    hbc = HbaseConnect(host="10.200.11.35", port=19090)

    with hbc.get_hbase_connect_pool().connection() as conn:
        a = conn.table('USER_ONE').row("0000000290064e8c9fb4a665ff37971b").get("INFO:REGION")
    print a
