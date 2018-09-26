#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 9/11/18 2:43 PM
# @Author  : Zengyu
# @Site    :
# @File    : update_one_row.py
# @Software: PyCharm
import uuid
import happybase
import re
from ConnectionUtil import MysqlConnect


def self_uid():
    """
    :return: 生成uuid
    """
    return uuid.uuid1().__str__().replace("-", "")


def merge_connect_tab(time):
    mysqlConn = MysqlConnect(host="root", user="Wahaha@123", db="Zy_sql")
    mysqlConn.execute_sql("""
    select group_concat(org_id),connect_id from CONNECT_TABLE 
    where status=1 and insert_time = '%s' group by connect_id """ % time)
    data = mysqlConn.cursor.fetchall()

    for row in data:
        org_ids = row[0].split(',')
        merge_hbase_data(org_ids, connect_id=row[1])
    mysqlConn.execute_sql("delete form CONNECT_TABLE WHERE status =1 and insert_time = '%s' " % time)

    pass


def merge_hbase_data(org_ids, connect_id=None):
    """
    :param org_ids: 需要合并的原始id集合
    :param connect_id: 关联id
    :return: void

    """
    hbase_conn = happybase.Connection(host="10.200.11.35", port=19090)
    if connect_id is None:
        connect_id = self_uid()
    USER_ONE = hbase_conn.table("USER_ONE")
    org_data = hbase_conn.table("ORG_DATA")
    MSG_ID_TAB = hbase_conn.table("MSG_ID")
    MESSAGE = hbase_conn.table("MESSAGE")
    GROUPMSG = hbase_conn.table("GROUPMSG")
    GROUPMSG_ID_TAB = hbase_conn.table("GROUPMSG_ID")
    ORG_CONNECT_TAB = hbase_conn.table("ORG_CONNECT_TAB")
    CONNECT_ORG_TAB = hbase_conn.table("CONNECT_ORG_TAB")
    CONTACT_LIST_NEW = hbase_conn.table("CONTACT_LIST_NEW")
    BE_CONTACT_TAB = hbase_conn.table("be_contact")
    new_data = {}
    # 0:直接合并的字段,1:需要累加求和的字段, 2:涉及到其他ID需要特殊处理的字段：
    merge_dic = {"INFO:REGION": 0, "INFO:PHONE": 0, "INFO:GENDER": 0, "INFO:BIRTHDAY": 0, "INFO:MAIL": 0,
                 "INFO:PLATFORM": 0, "INFO:ADDRESS": 0, "INFO:WORD_LIST": 0,
                 "INFO:CONTACT_COUNT": 1, "INFO:BE_CONTACT_COUNT": 1, "INFO:MSG_COUNT": 1,
                 "INFO:RELATION": 2, "INFO:NAME": 2, "INFO:CONTACT": 2, "INFO:BE_CONTACT": 2
                 }
    print("Start to merge ids ! ")
    for rowkey in org_ids:
        data = USER_ONE.row(rowkey)
        # mids = []
        # group_mids = []
        for key in data:
            print("Start to merge %s" % key)
            dic = merge_dic.get(key)
            if dic == 0:
                if new_data.get(key) is None:
                    new_data[key] = data.get(key)
                else:
                    new_data[key] = new_data.get(key) + '\x02' + data.get(key)
            elif dic == 1:
                if new_data.get(key) is None:
                    new_data[key] = data.get(key)
                else:
                    new_data[key] = str(int(new_data.get(key)) + int(data.get(key)))
                if key == "INFO:MSG_COUNT":
                    msg_results = MSG_ID_TAB.scan(row_prefix=rowkey)

                    for result in msg_results:
                        mid = result[1].get("INFO:MID")
                        row = MESSAGE.row(mid)
                        if row.get("INFO:user") == rowkey:
                            row["INFO:user"] = connect_id
                            new_mid = '-'.join([connect_id, row.get("INFO:time"), row.get("INFO:platform_msg_id")])
                            MESSAGE.put(b'%s' % new_mid, row)
                            MESSAGE.delete(mid)
                        elif row.get("INFO:party") == rowkey:
                            row["INFO:party"] = connect_id
                            MESSAGE.put(b'%s' % connect_id, row)
                    groupmsg_results = GROUPMSG_ID_TAB.scan(row_prefix=rowkey)
                    for result in groupmsg_results:
                        mid = result[1].get("INFO:MID")
                        row = GROUPMSG.row(mid)
                        row["INFO:sender"] = connect_id
                        GROUPMSG.put(mid, row)

            if dic == 2:
                column = data.get(key)
                if key == "INFO:NAME":
                    new_names_l = []
                    for names in column.split('\x02'):
                        p = re.compile("name:(.*)\\|platforms:(.*)\\|score:(.*)")

                        matcher = re.match(p, names)
                        name, platforms, score = matcher.groups()
                        new_platforms = []
                        for pl in platforms.split(','):
                            source_id, platform = pl.split('-')
                            new_source_id = ORG_CONNECT_TAB.row(source_id).get("CONNECT_ID:ID")
                            if new_source_id is not None:
                                source_id = new_source_id
                            new_pl = source_id + '-' + platform
                            new_platforms.append(new_pl)
                        new_platforms = ','.join(new_platforms)
                        new_names = "name:" + name + "|platforms:" + new_platforms + "|score:" + score
                        new_names_l.append(new_names)
                    if new_data.get(key) is None:
                        new_data[key] = '\x02'.join(new_names_l)
                    else:
                        new_data[key] = new_data.get(key) + '\x02' + '\x02'.join(new_names_l)
                elif key == "INFO:CONTACT":
                    new_contacts = []
                    for contacts in column.split('\x02'):
                        contact, platform = contacts.split('|')
                        c_id = ORG_CONNECT_TAB.row(contact).get("CONNECT_ID:ID")
                        if c_id is not None:
                            contact = c_id

                        #  修改contact对应ROWKEY自己在USER_ONE 中的be_contact column
                        be_contact = USER_ONE.row(contact).get("INFO:BE_CONTACT")
                        if be_contact.__contains__(rowkey):
                            be_contact = be_contact.replace(rowkey, connect_id)  # 把be_contact 中的org_id 改为connect_id
                            USER_ONE.put(contact, {b'INFO:BE_CONTACT': be_contact})

                        # 替换CONTACT_LIST_NEW 中的id
                        contact_list_rowkey = rowkey + '-' + contact
                        value = CONTACT_LIST_NEW.row(contact_list_rowkey)
                        CONTACT_LIST_NEW.put(connect_id + '-' + contact, value)
                        CONTACT_LIST_NEW.delete(contact_list_rowkey)

                        # 替换be_contact中的id
                        be_contact_rowkey = contact + '-' + rowkey
                        be_contact_v = BE_CONTACT_TAB.row(be_contact_rowkey)
                        BE_CONTACT_TAB.put(contact + '-' + connect_id, be_contact_v)
                        BE_CONTACT_TAB.delete(be_contact_rowkey)

                        new_contacts.append(contact + '|' + platform)

                    if new_data.get(key) is None:
                        new_data[key] = '\x02'.join(new_contacts)
                    else:
                        new_data[key] = new_data.get(key) + '\x02' + '\x02'.join(new_contacts)

                    pass
                if key == "INFO:BE_CONTACT":
                    new_be_contacts = []
                    for v in column.split('\x02'):
                        index = v.find('|')
                        id = v[:index]
                        print("id is %s" % id)
                        if ORG_CONNECT_TAB.row(id).get("CONNECT_ID:ID") is not None:
                            id = ORG_CONNECT_TAB.row(id).get("CONNECT_ID:ID")
                        # 修改be_contact 对应ROWKEY自己在USER_ONE 中的 contact column
                        contact_value = USER_ONE.row(id).get("INFO:CONTACT")
                        print("contact_value is %s" % contact_value)
                        if contact_value.__contains__(rowkey):
                            contact_value = contact_value.replace(rowkey, connect_id)
                            print("find !")
                            USER_ONE.put(id, {b'INFO:CONTACT': contact_value})

                        # 替换CONTACT_LIST_NEW 中的id
                        contact_list_rowkey = id + '-' + rowkey
                        value = CONTACT_LIST_NEW.row(contact_list_rowkey)
                        CONTACT_LIST_NEW.put(id + '-' + connect_id, value)
                        CONTACT_LIST_NEW.delete(contact_list_rowkey)
                        # 替换be_contact中的id
                        be_contact_rowkey = rowkey + '-' + id
                        be_contact_v = BE_CONTACT_TAB.row(be_contact_rowkey)
                        BE_CONTACT_TAB.put(connect_id + '-' + id, be_contact_v)
                        BE_CONTACT_TAB.delete(be_contact_rowkey)

                        new_be_contacts.append(id + v[index:])
                    if new_data.get(key) is None:
                        new_data[key] = '\x02'.join(new_be_contacts)
                    else:
                        new_data[key] = new_data.get(key) + '\x02' + '\x02'.join(new_be_contacts)
                    pass

                pass
            else:
                pass

        # 把原始数据放入org_data
        # 需要判断ROWKEY 是否是已经合并过的ID,若为已合并的 需要将ROWKEY 拆分后再存入 ORG_CONNECT_ID 和ORG_DATA ,CONNECT_ORG_TAB
        org_org_ids = CONNECT_ORG_TAB.row(rowkey).get("ORG_ID:ID")
        if org_org_ids is None:
            print("put org_data %s to ORG_DATA" % rowkey)
            org_data.put(rowkey, data)
            print("put connect_id %s to ORG_CONNECT_TAB " % connect_id)
            ORG_CONNECT_TAB.put(rowkey, {b'CONNECT_ID:ID': connect_id})
        else:
            # 删除org_ids,CONNECT_ORG_TAB 中的合并id
            CONNECT_ORG_TAB.delete(rowkey)
            org_ids.remove(rowkey)
            for id in org_org_ids:
                # 细化到原始ID
                org_ids.append(id)
                ORG_CONNECT_TAB.put(id, {'CONNECT_ID:ID': connect_id})

    print("put org_ids to CONNECT_ORG_TAB ")
    CONNECT_ORG_TAB.put(connect_id, {b"ORG_ID:ID": '\t'.join(org_ids)})
    USER_ONE.put(connect_id, new_data)

    hbase_conn.close()

    pass


