#!/usr/bin/env python
# coding: utf-8
# @Time    : 2018/8/15 17:44
# @Author  : zeng yu 
# @Site    : 
# @File    : loadData.py
# @Software: PyCharm


def load_data_from_mysql():
    """
    从 Mysql读取数据 装载到Hive
    """

    import os
    from pyhive import hive

    conn = hive.connect(host="192.168.8.94", port=10000, authMechanism="PLAIN", user="hdfs")
    mysql_info = {"host": "192.168.1.200", "port": 3306, "user": "root", "passwd": "123456"}

    def run_hive_query(sql):
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def mysql_to_hive(host, port, user, passwd, database, table):
        # os.system("hadoop fs -rm    -r /user/task/%s"%table)
        if [database] not in run_hive_query("show databases"):
            with conn.cursor() as cursor:
                cursor.execute("create database " + database)
        with conn.cursor() as cursor:
            cursor.execute("use  " + database)
        if [table] not in run_hive_query("show tables"):
            os.system(
                "sqoop  import --connect   jdbc:mysql://%s:%s/%s --username  %s   --password  %s --table %s  --hive-database  %s  -m 10 --create-hive-table --hive-import   --hive-overwrite " % (
                    host, port, database, user, passwd, table, database))
        else:
            os.system(
                "sqoop   import --connect   jdbc:mysql://%s:%s/%s --username  %s   --password  %s --table %s  --hive-database  %s  -m 10 --hive-import   --hive-overwrite " % (
                    host, port, database, user, passwd, table, database))

    mysql_to_hive(mysql_info["host"], mysql_info["port"], mysql_info["user"], mysql_info["passwd"].replace("(", "\("),
                  "wwn", "cm_vip")

    pass


def load_data_from_csv():
    """
    读取csv文件
    :return:
    """
    pass


def load_data_from_json():
    """
    读取json文件
    :return:
    """
    pass
