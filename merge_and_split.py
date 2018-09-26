#!/usr/bin/env python
# coding: utf-8
# @Time    : 2018/9/4 18:06
# @Author  : zeng yu 
# @Site    : 
# @File    : merge_and_split.py
# @Software: PyCharm
import uuid

import happybase


HBASE_CONN = happybase.Connection(host="10.200.11.35", port=19090)


def merge(*uids):
    """
    :param uids: 需要合并信息的uid
    :return:
    """
    USER_ONE = HBASE_CONN.table("USER_ONE")
    new_uid = uuid.uuid1().__str__().replace("-", "")
    new_value = {}
    for uid in uids:
        value = USER_ONE.row(uid)


def split(uid):
    """
    :param uid: 需要拆分的Uid
    :return:
    """

    pass


