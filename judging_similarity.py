#!/usr/bin/env python
# coding: utf-8
# @Time    : 2018/9/11 15:57
# @Author  : zeng yu 
# @Site    : 
# @File    : judging_similarity.py
# @Software: PyCharm

from __future__ import division

from pyspark import SparkConf, SparkContext, HiveContext, Row


def judge_similarity(**kwargs):
    """
    比较2个人的相似程度
    :param kwargs:
    :return: 相似度
    """
    user1 = kwargs.get('user1')
    user2 = kwargs.get('user2')
    user1_phones, user2_phones = user1.get('phone'), user2.get('phone')
    user1_names, user2_names = user1.get('name'), user2.get('name')
    user1_mails, user2_mails = user1.get('mail'), user2.get('mail')
    user1_contact, user2_contact = user1.get('contact_list'), user2.get('contact_list')

    def get_score(list1, list2, weight=0.0):
        total_size = set(list1 + list2).__len__()

        intersection = list(set(list1).intersection(list2))

        s = 100 * weight * (intersection.__len__() / total_size)

        return s

    score = get_score(user1_phones, user2_phones, 0.3) + get_score(user1_names, user2_names, 0.3) + get_score(
        user1_mails, user2_mails, 0.2) + get_score(user1_contact, user2_contact, 0.2)

    return score


def get_compare():
    get_id = hc.sql("select a.phone as gid,rowkey from  "
                    "(select phone,collect_set(rowkey) rowkeys  from new_type.user group by phone having size(c)>1) a "
                    "lateral view explode(a.rowkeys) tab as rowkey   ")
    get_id.cache()
    users = hc.sql("select rowkey,collect_set(name) name,collect_set(phone) phone ,collect_set(mail)  mail  "
                   "from new_type.user  group by rowkey").alias('a')
    contacts = hc.sql('select u_uid,collect_set(c_uid) contacts from new_type.contact group by u_uid  ')
    user_compare = get_id.join(users, 'rowkey', 'inner').select('a.rowkey', 'gid', 'name', 'phone', 'mail').join(contacts, 'rowkey',
                                                                                                  'inner').select(
        'a.rowkey', 'gid', 'name', 'phone', 'mail', 'contacts')

    def func(iter):
        result = {}
        for i in range(0, iter.__len__()):
            
            pass

    user_compare.rdd.groupBy(lambda x:x.gid).map(func)


if __name__ == '__main__':
    # user1 = {'phone': ['123'], 'name': ['zy'], 'mail': ['abc@123.com'], 'contact_list': ['asd', 'dasdas']
    #
    #          }
    # user2 = {'phone': ['123'], 'name': ['zy', 'zy3'], 'mail': ['abc@123.com'], 'contact_list': ['dasdas']
    #
    #          }
    #
    # a = judge_similarity(user1=user1, user2=user2)
    # print a
    sc = SparkContext(conf=SparkConf())
    hc = HiveContext(sc)
