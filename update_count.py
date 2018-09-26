#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 5/14/18 10:48 PM
# @Author  : Zengyu
# @Site    : 
# @File    : update_count.py
# @Software: PyCharm
import time

import MySQLdb
from pyhive import hive


class HiveConn(object):
    def __init__(self):
        self.conn = hive.Connection(host='10.200.11.35',
                                    port=10000,
                                    username='root',
                                    database='beetalks')
        self.cursor = self.conn.cursor()

    def execute_sql(self, string):
        self.cursor.execute(string)
        return self.cursor.fetchall()


class MysqlConn(object):

    def __init__(self, user, password, db):
        self.conn = MySQLdb.connect(
            host='10.200.110.16',
            port=3306,
            user=user,
            passwd=password,
            db=db,
            charset='utf8',
        )
        self._cursor = self.conn.cursor()

    def update(self, u_count, c_count, m_count, g_count, time):

        try:
            # 执行sql语句
            self._cursor.execute("""
                   replace INTO update_count(user_count,contact_count,message_count,groupmessage_count,time) 
                   values ('%d' ,'%d', '%d', '%d','%s' ) 

                   """ % (u_count, c_count, m_count, g_count, time))
            # 提交到数据库执行
            self.conn.commit()
        except:
            # Rollback in case there is any error
            print '插入数据失败!'
            self.conn.rollback()

    def getdata(self, timestamp):
        self._cursor.execute("""
        select sum(user_count),sum(contact_count) from update_count where time >'%s'
        """ % timestamp)

        results = self._cursor.fetchall()
        u_count = 0
        c_count = 0
        for row in results:
            u_count = row[0]
            c_count = row[1]
        return u_count, c_count

    def week_count(self, *args, **kwargs):
        try:
            self._cursor.execute("""
                replace into  week_count(user_count,contact_count,txt_count,pic_count,video_count,wav_count,time) 
                values (%d ,%d,%d,%d ,%d,%d,%s ) 
            """ % args)
            # 提交到数据库执行
            self.conn.commit()
        except:
            # Rollback in case there is any error
            print '插入数据失败!'
            self.conn.rollback()

    def update_detail(self, args):
        try:
            self._cursor.execute("""
                replace into  update_detail(data_name,count,type,source,location,insert_time) 
                values (%d ,%d,%d,%d ,%d,%d,%s ) 
            """ % args)
            # 提交到数据库执行
            self.conn.commit()
        except:
            # Rollback in case there is any error
            print '插入数据失败!'
            self.conn.rollback()

        pass


if __name__ == '__main__':
    hc = HiveConn()
    def update_count(t):

        u_count = hc.execute_sql(""" select count(1) from 
        (select a.uid from (select uid from beetalks.src_phone where time like '{0}%' 
        union all 
        select uid from beetalks.src_mail where data_time like '{0}%'  )a group by a.uid) c """.format(t))[0][0]
        print("u_count = ",u_count)
        c_count = hc.execute_sql("""
        select count(1) from (select distinct * from beetalks.src_contact where time like '{0}%') a
        """.format(t))[0][0]
        print 'c_count = ',c_count
        m_count = hc.execute_sql("""
         select count(1) from (select mid from src_message where data_time like '{0}%' group by mid )a
        """.format(t))[0][0]
        print("m_count = ", m_count)
        g_count =hc.execute_sql("""
          select count(1) from (select mid from src_groupmsg where data_time like '{0}%' group by mid )a
        """.format(t))[0][0]
        print("g_count = ", g_count)
        t = time.strptime(t, '%Y%m%d')
        t = str(int(time.mktime(t))) + '000'
        MysqlConn('sonar', 'sonar&404', 'sonar').update(u_count, c_count, m_count, g_count, t)
        MysqlConn('sonar', 'sonar&404', 'sonar_dev').update(u_count, c_count, m_count, g_count, t)
        MysqlConn('sonar', 'sonar&404', 'sonar_test').update(u_count, c_count, m_count, g_count, t)

        pass

    def week_count(t, timestamp):


        # msgtype = hc.execute_sql("""
        # select msgtype,count(1) from beetalks.src_message where substr(data_time,0,8)>'20180516'
        # group by msgtype
        # """)
        # print(msgtype)
        t = time.strptime(t, '%Y%m%d')
        t = str(int(time.mktime(t))) + '000'
        a = MysqlConn('sonar', 'sonar&404', 'sonar')
        u_count_t = a.getdata(timestamp)[0]
        c_count_t = a.getdata(timestamp)[1]

        u_count = 2055131992+u_count_t
        c_count = 12140402832+c_count_t
        t_count = 2657456+591828
        p_count = 1754450+232048
        v_count = 71146+8816
        w_count = 388313+75634
        print(u_count)
        print(c_count)
        MysqlConn('sonar', 'sonar&404', 'sonar').week_count(u_count, c_count, t_count, p_count, v_count, w_count, t)
        MysqlConn('sonar', 'sonar&404', 'sonar_dev').week_count(u_count, c_count, t_count, p_count, v_count, w_count, t)
        MysqlConn('sonar', 'sonar&404', 'sonar_test').week_count(u_count, c_count, t_count, p_count, v_count, w_count, t)

    def detail_count(tablename='' ,dataname='', type='', count=0, source='', time='',partition_name='',insert_time=''):
        count = hc.execute_sql("select count(1) from %s where %s >'%s' " % tablename % partition_name % time)
        return dataname, type, count, source, insert_time

        pass

    t = time.strftime("%Y%m%d", time.localtime())
    # t = time.strftime("%Y%m%d", time.localtime())
    # t = "20180712"
    update_count(t)
    # week_count(t,'1526454000000')