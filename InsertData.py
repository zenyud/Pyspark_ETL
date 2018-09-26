#!/usr/bin/env python
# coding: utf-8
# @Time    : 2018/8/15 17:47
# @Author  : zeng yu 
# @Site    : 
# @File    : InsertData.py
# @Software: PyCharm

"""
    执行execute_load_data() 方法 即可进行hbase数据的装载.
"""
import datetime
import json
from ConnectionUtil import *
from util import get_catlog
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, BooleanType, FloatType, IntegerType, LongType, DateType, TimestampType, Row, \
    StructField, StructType
from config import *
from pyspark.sql import DataFrame
from pyspark import SparkConf, SparkContext, HiveContext, RDD
from transformData import USER_TITLE, exception, logger, execute_func
import sys

TYPE_DICT = {"string": StringType(), "boolean": BooleanType(), "float": FloatType(), "int": IntegerType(),
             "long": LongType(), "date": DateType(), "datetime": TimestampType()}

TITTLE = USER_TITLE
CONNECT_TABLE_CATELOG = {"table": {"namespace": "default", "name": "ORG_CONNECT_TAB"}, "rowkey": "", "domain": "",
                         "columns": {"org_id": {"cf": "rowkey", "col": "KEY", "type": "string"},
                                     "connect_id": {"cf": "CONNECT_ID", "col": "ID", "type": "string"}, }}
HBASE_CATELOG = get_catlog(*TITTLE, table_name="USER_TEST", family="INFO", rowkey="rowkey")
now = datetime.datetime.now().date()
date = now - datetime.timedelta(days=1)
# TIME = str(now).replace("-", "")
# DATE = str(date).replace("-", "")
TIME = "20180606"


def transfer_string(v, type):
    value = ''
    if not v:
        value = ''
    elif type == 'boolean':
        value = v and 'True' or 'False'
    elif type == 'date':
        value = v.strftime("%Y-%m-%d")
    elif type == 'datetime':
        value = v.strftime("%Y-%m-%d %H:%M:%S")
    else:
        value = '%s' % v
    return value


class LoadDataToHbase(object):
    """
        数据装载
    """

    def __init__(self):
        pass

    # 数据join  关联ID表,将合并后的数据插入到hbase当中.
    # 关联ID表  org_connect_id connect_org_id

    @staticmethod
    def join_id(insert_data, *cols):
        """
        :param insert_data: SPARK DataFrame
        :return: join 了关联表的DataFrame
        """
        reload(sys)
        sys.setdefaultencoding('utf-8')

        org_connect_df = shb.get_df_from_hbase(CONNECT_TABLE_CATELOG)  # 关联表
        # insert_data.show(10, False)
        result = insert_data.join(org_connect_df, org_connect_df.org_id == insert_data.rowkey, "left").selectExpr(*cols)
        return result

    @staticmethod
    def merge_hbase_data(insert_data, user_title, merge_name):
        """
        :param merge_name: 合并字段名
        :param user_title: 合并数据cols 集合
        :param insert_data:已经关联Id后的 data
        :return: DataFrame
        """
        model = Row(*user_title)
        struct_fields = [StructField(x, StringType(), True) for x in user_title]
        schema = StructType(struct_fields)

        def merge(iter):
            reload(sys)
            sys.setdefaultencoding('utf-8')
            hbase_conn_pool = HbaseConnect(host="10.200.11.35", port=19090).get_hbase_connect_pool()
            with hbase_conn_pool.connection() as conn:
                table = conn.table('USER_TEST')
                for row in iter:
                    result = {}
                    uid, local_all_value = row
                    result["rowkey"] = uid
                    hbase_all_value = table.row(uid)
                    # 将json 字符串转换成python对象
                    if hbase_all_value:
                        for key in local_all_value.iterkeys():
                            if key == 'contact_count':
                                continue
                            # 比如 key 为 name
                            local_value = local_all_value[
                                key]  # {zy:{'platform':[{'platform':'tango','source_id':'qwz'}],'score':1}}
                            hbase_value = hbase_all_value.get("INFO:%s" % str(key).upper())
                            new_value = {}  # 合并后的name_dict
                            if hbase_value is not None:
                                hbase_value = json.loads(hbase_value)
                                new_value.update(hbase_value)  # 先存入hbase的value
                            if local_value:
                                for l_key in local_value.iterkeys():
                                    if l_key in new_value:
                                        # 如果Hbase 存在相同的值，则需对其进行合并
                                        l_platforms = local_value.get(l_key).get(merge_name)
                                        h_platforms = new_value[l_key].get(merge_name)
                                        # h_platforms 是一个LIST 如[{'platform':'tango','source_id':'qwz'},{'platform':'tango','source_id':'sadas'}]
                                        for p in l_platforms:
                                            if not h_platforms.__contains__(p):
                                                h_platforms.append(p)
                                        new_value[l_key] = {merge_name: h_platforms, "score": h_platforms.__len__()}
                                    else:
                                        new_value[l_key] = local_value[l_key]  # 无需合并
                            if new_value:
                                # 如果new_value 不为空,就将其插入 Result中
                                result[key] = new_value

                    else:
                        result.update(local_all_value)
                    if "contacts" in result:
                        result["contact_count"] = result.get("contacts").__len__()
                    if "be_contacts" in result:
                        result["be_contact_count"] = result.get("be_contacts").__len__()

                    data = [json.dumps(result.get(t)) if result.get(t) is not None else None for t in user_title]
                    data[0] = json.loads(data[0])  # rowkey 无需转换成json对象
                    yield model(*data)

        c = insert_data.mapPartitions(merge)
        df = sqlcontext.createDataFrame(c, schema=schema)
        return df

    def insert_user_info_to_hbase(self, insert_data):
        """
        :param insert_data: User_data
        :return: 数据插入Hbase
        """

        def func(x):
            """
            对聚合后的数据进行格式化
            :param x: (uid,ResultIterable)
            :return: uid,{result}
            result 的格式为：
             {"name":
                {   "zy":{"platform":[{'source_id':'qweqw','platform':'tango'}],"score":1},
                    "tl":{"platform":[{'source_id':'qweqw','platform':'tango'},{'source_id':'dasdsad','platform':'tango'}],
                            "score":2}
                }
             }
            """
            _result = {}
            rowkey = x[0]
            values = x[1]  # Iter or Row[]
            row_dict = list(values)[0].asDict()

            special_cols = ["rowkey", 'name', 'userid', 'platform', 'salt', 'password', 'reg_time', 'name_source',
                            'is_user']
            for key in row_dict.iterkeys():
                if key not in special_cols:
                    items = [(row[key], row["platform"]) for row in values]
                    item_dict = {}
                    for item, platform in items:
                        if item_dict.get(item) is None:
                            if item is None or item == 'null':
                                continue
                            item_dict[item] = {"platform": [platform], "score": 1}
                        else:
                            c = item_dict[item]["platform"]
                            if not c.__contains__(platform):
                                c.append(platform)
                                item_dict[item] = {"platform": c, "score": c.__len__()}
                    _result[key] = item_dict if item_dict.__len__() != 0 else None
                elif key == "name":
                    names = [(row.name, row.name_source, row.platform) for row in values]
                    names = set(names)
                    name_dict = {}
                    for name, source, platform in names:
                        if name_dict.get(name) is None:
                            if name is None or name == 'null':
                                continue
                            name_dict[name] = {"platform": [{"source_id": source, "platform": platform}], "score": 1}
                        else:
                            c = name_dict.get(name).get("platform")
                            c.append({"source_id": source, "platform": platform})
                            name_dict[name] = {"platform": c, "score": c.__len__()}
                    _result[key] = name_dict if name_dict.__len__() != 0 else None

                elif key == 'platform':
                    platform_infos = [(row["platform"], row['userid'], row["salt"], row["password"], row['reg_time'])
                                      for row in values]
                    platform_infos = set(platform_infos)
                    item_dict = {}
                    for platform, userid, salt, password, reg_time in platform_infos:
                        if item_dict.get(platform) is None:
                            if platform is None or platform == 'null':
                                continue
                            item_dict[platform] = {"platform": [
                                {'userid': userid, "salt": salt, "password": password, "reg_time": reg_time}],
                                "score": 1}
                        else:
                            c = item_dict[platform]["platform"]
                            c.append({'userid': userid, "salt": salt, "password": password, "reg_time": reg_time})
                            item_dict[platform] = {"platform": c, "score": c.__len__()}
                    _result[key] = item_dict if item_dict.__len__() != 0 else None

                    pass
            return rowkey, _result

        join_title = ["nvl(connect_id,rowkey) as rowkey ", "userid", "name", "mail", "phone", "platform", "reg_time",
                      "password", "salt", "gender", "birthday", "address", "area", "profession", "card", "facebookid",
                      "ip", "lives", "race", "status", "name_source"]

        tmp = self.join_id(insert_data, *join_title)
        result = tmp.rdd.groupBy(lambda x: x.rowkey).map(func)
        result = self.merge_hbase_data(result, TITTLE, "platform")

        shb.save_df_to_hbase(result, get_catlog(*TITTLE, table_name="USER_TEST", rowkey="rowkey", family="INFO"))

    def insert_region_info_to_hbase(self, insert_data=DataFrame):
        """
        :param insert_data: Region 表
        :return: 插入状态
        """
        join_title = ["nvl(connect_id, rowkey) as rowkey", "country", "province", "city", "phone", "code", "platform"]
        tmp = self.join_id(insert_data, *join_title)

        def func(x):
            _result = {}
            rowkey = x[0]
            values = x[1]  # Iter or Row[]
            row_dict = list(values)[0].asDict()
            for key in row_dict.iterkeys():
                if key == 'phone':
                    regions = [(row.phone, row.country, row.province, row.city, row.code, row.platform) for row in
                               values]
                    regions = set(regions)
                    item_dict = {}
                    for phone, country, province, city, code, platform in regions:
                        if item_dict.get(phone) is None:
                            if phone is None or phone == 'null':
                                continue
                            item_dict[phone] = {"regions": [
                                {"country": country, "province": province, "city": city, "code": code,
                                 "platform": platform}], "score": 1}
                        else:
                            c = item_dict.get(phone).get("regions")
                            c.append({"country": country, "province": province, "city": city, "code": code,
                                      "platform": platform})
                            item_dict[phone] = {"regions": c, "score": c.__len__()}
                    _result["region"] = item_dict if item_dict.__len__() != 0 else None
            return rowkey, _result

        result = tmp.rdd.groupBy(lambda x: x.rowkey).map(func)
        region_titile = ["rowkey", "region"]

        result = self.merge_hbase_data(result, region_titile, "regions")

        shb.save_df_to_hbase(result, get_catlog(*region_titile, table_name="USER_TEST", rowkey="rowkey", family="INFO"))

    def insert_contact_to_hbase(self, contact_data):
        """
        插入联系人表信息到Hbase
        :param contact_data :联系人数据
        :return:
        """
        reload(sys)
        sys.setdefaultencoding('utf-8')
        org_connect_df = shb.get_df_from_hbase(CONNECT_TABLE_CATELOG)  # 关联表
        # insert_data.show(10, False)
        cols = ["nvl(connect_id,u_uid) u_uid", "c_uid", "name", "platform"]
        cols2 = ["u_uid", "nvl(connect_id,c_uid)  c_uid", "name", "platform"]
        input_da = contact_data.join(org_connect_df, org_connect_df.org_id == contact_data.u_uid, "left").selectExpr(
            *cols)
        tmp = input_da.join(org_connect_df, org_connect_df.org_id == contact_data.c_uid, "left").selectExpr(*cols2)
        tmp.cache()

        def func(x):
            _result = {}
            rowkey = x[0]
            values = x[1]
            contacts = [(row.c_uid, row.name, row.platform) for row in values]
            regions = set(contacts)
            item_dict = {}
            for c_uid, name, platform in regions:
                if item_dict.get(c_uid) is None:
                    if c_uid is None or c_uid == 'null':
                        continue
                    item_dict[c_uid] = {"contact_info": [{"name": name, "platform": platform}], "score": 1}
                else:
                    c = item_dict.get(c_uid).get("contact")
                    c.append({"name": name, "platform": platform})
                    item_dict[c_uid] = {"contact_info": c, "score": c.__len__()}
            _result["contacts"] = item_dict if item_dict.__len__() != 0 else None
            _result["contact_count"] = item_dict.__len__()
            return rowkey, _result

        def func2(x):
            _result = {}
            rowkey = x[0]
            values = x[1]
            contacts = [(row.u_uid, row.name, row.platform) for row in values]
            regions = set(contacts)
            item_dict = {}
            for u_uid, name, platform in regions:
                if item_dict.get(u_uid) is None:
                    if u_uid is None or u_uid == 'null':
                        continue
                    item_dict[u_uid] = {"be_contact_info": [{"name": name, "platform": platform}], "score": 1}
                else:
                    c = item_dict.get(u_uid).get("contact")
                    c.append({"name": name, "platform": platform})
                    item_dict[u_uid] = {"be_contact_info": c, "score": c.__len__()}
            _result["be_contacts"] = item_dict if item_dict.__len__() != 0 else None
            _result["be_contact_count"] = item_dict.__len__()
            return rowkey, _result

        contact_titile = ["rowkey", "contacts", "contact_count"]
        be_contact_titile = ["rowkey", "be_contacts", "be_contact_count"]
        contact_result_tmp = tmp.rdd.groupBy(lambda x: x.u_uid).map(func)
        be_contact_result_tmp = tmp.rdd.groupBy(lambda x: x.c_uid).map(func2)

        contact_result = self.merge_hbase_data(contact_result_tmp, contact_titile, "contact_info")
        be_contact_result = self.merge_hbase_data(be_contact_result_tmp, be_contact_titile, "be_contact_info")
        shb.save_df_to_hbase(contact_result,
                             get_catlog(*contact_titile, table_name="USER_TEST", rowkey="rowkey", family="INFO"))
        shb.save_df_to_hbase(be_contact_result,
                             get_catlog(*be_contact_titile, table_name="USER_TEST", rowkey="rowkey", family="INFO"))

    def insert_message_to_hbase(self, message_data):
        """
        插入聊天记录到Hbase,聊天记录无需合并
        :param message_data: 聊天记录数据
        :return:
        """
        message_titile = ["mid", "content", "platform_msg_id", "partyuid", "useruid", "party", "user", "timesent",
                          "media", "direction", "msgtype", "platform"]
        org_connect_df = shb.get_df_from_hbase(CONNECT_TABLE_CATELOG)  # 关联表

        msg_tmp = message_data.join(org_connect_df, org_connect_df.org_id == message_data.user, "left").selectExpr(
            "contact_ws('-',nvl(connect_id,user),timesent,platform_msg_id) mid", "content", "platform_msg_id",
            "partyuid", "useruid", "party", "nvl(connect_id,user) user", "timesent", "media", "direction", "msgtype",
            "platform")
        msg_result = msg_tmp.join(org_connect_df, org_connect_df.org_id == message_data.user, "left").selectExpr("mid",
                                                                                                                 "content",
                                                                                                                 "platform_msg_id",
                                                                                                                 "partyuid",
                                                                                                                 "useruid",
                                                                                                                 "nvl(connect_id,party)  party",
                                                                                                                 "user",
                                                                                                                 "timesent",
                                                                                                                 "media",
                                                                                                                 "direction",
                                                                                                                 "msgtype",
                                                                                                                 "platform")

        shb.save_df_to_hbase(msg_result,
                             get_catlog(*message_titile, table_name="MESSAGE_TEST", rowkey="mid", family="INFO"))


    def insert_groupmsg_to_hbase(self, groupmsg_data):
        """
        插入群聊记录到HBase
        :param groupmsg_data : 群聊天记录数据
        :return:
        """
        groupmsg_titile = ["mid", "gid", "platform_sender_id", "sender", "platform_msg_id", "content", "timesent",
                           "media", "msgtype", "platform"]
        org_connect_df = shb.get_df_from_hbase(CONNECT_TABLE_CATELOG)  # 关联表
        groupmsg_result = groupmsg_data.join(org_connect_df.org_id == groupmsg_data.sender, "left").selectExpr("mid", "gid",
                                                                                             "platform_sender_id",
                                                                                             "nvl(connect_id,sender) sender",
                                                                                             "platform_msg_id",
                                                                                             "content"
                                                                                             "timesent", "media",
                                                                                             "msgtype", "platform")
        shb.save_df_to_hbase(groupmsg_result,
                             get_catlog(*groupmsg_titile, table_name="GROUP_MESSAGE_TEST", rowkey="mid", family="INFO"))
        pass

    def insert_groupinfo_to_hbase(self, groupinfo_data):
        """
        :param groupinfo_data: 群组信息
        :return:
        """
        groupinfo_title = []
        org_connect_df = shb.get_df_from_hbase(CONNECT_TABLE_CATELOG)
        groupinfo_result = groupinfo_data.join()

        pass

    def insert_groupmember_to_hbase(self, groupmember_data):
        """
        :param groupmember_data: 群组人员信息
        :return:
        """
        pass

    def insert_statistics_info_to_hbase(self):
        """
        插入统计信息到Hbase
        :param
        :return:
        """
        pass


class SparkHbaseConn(object):
    def __init__(self, sc, sqlContext, conf=CONF):
        self.conf = conf
        self.prefix = conf.get('prefix', '')
        self.sc = sc
        self.sqlContext = sqlContext
        self.cache_rdd = {}

    def save_df_to_hbase(self, dataframe, catelog):
        """
        :param dataframe: :class:`DataFrame`
        :param catelog: 数据格式
        :return:
        """

        def f(rdds):

            reload(sys)
            sys.setdefaultencoding('utf-8')
            columns = catelog["columns"]
            rowkey = catelog["rowkey"]
            newrdds = []
            for rdd in rdds:
                rdd_dict = rdd.asDict()
                key = str(rdd_dict[rowkey])
                for k, v in rdd_dict.items():
                    if k in columns and k != rowkey and v is not None:
                        newrdds.append(
                            (key, [key, columns[k]['cf'], columns[k]['col'], transfer_string(v, columns[k]['type'])]))
            return newrdds

        conf = self.conf.copy()
        prefix = catelog['table']['namespace'] == "default" and self.prefix or catelog['table']['namespace']
        table = catelog['table']['name']
        conf.update({"mapreduce.outputformat.class": OUTPUTFORMATCLASS, "mapreduce.job.output.key.class": KEYCLASS,
                     "mapreduce.job.output.value.class": VALUECLASS})
        conf['hbase.mapred.outputtable'] = prefix and "%s:%s" % (prefix, table) or table
        try:
            dataframe.rdd.mapPartitions(f).saveAsNewAPIHadoopDataset(conf=conf, keyConverter=OUTKEYCONV,
                                                                     valueConverter=OUTVALUECONV)
        except Exception:
            return False
        return True

    def get_df_from_hbase(self, catelog, cached=True):
        """
        :param catelog: json , eg:
        {
            "table":{"namespace":"default", "name":"table1"},
            "rowkey":"col0",
            "domain": "id > 1 and skuid = 124 "
            "columns":{
              "col0":{"cf":"rowkey", "col":"key", "type":"string"},
              "col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
              "col3":{"cf":"cf3", "col":"col3", "type":"float"},
              "col4":{"cf":"cf4", "col":"col4", "type":"int"},
              "col5":{"cf":"cf5", "col":"col5", "type":"long"},
              "col7":{"cf":"cf7", "col":"col7", "type":"date"},
              "col8":{"cf":"cf8", "col":"col8", "type":"datetime"}
            }
        }
        :return: :class:`DataFrame`
        """
        conf = self.conf.copy()
        prefix = catelog['table']['namespace'] == "default" and self.prefix or catelog['table']['namespace']
        table = catelog['table']['name']
        conf['hbase.mapreduce.inputtable'] = prefix and "%s:%s" % (prefix, table) or table
        hbase_rdd = self.sc.newAPIHadoopRDD(INPUTFORMATCLASS, KEYCLASS, VALUECLASS, keyConverter=INKEYCONV,
                                            valueConverter=INVALUECONV, conf=conf)

        def value_type_transfer(v, type):
            value = None
            if v:
                try:
                    if type == 'string':
                        value = v
                    elif type == 'boolean':
                        value = bool(v)
                    elif type == 'float':
                        value = float(v)
                    elif type == 'int':
                        value = int(v)
                    elif type == 'long':
                        value = long(v)
                    elif type == 'date':
                        value = datetime.datetime.strptime(v, '%Y-%m-%d')
                    elif type == 'datetime':
                        value = datetime.datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
                    else:
                        value = v
                except:
                    value = None
            return value

        def hrdd_to_rdd(rdds):
            new_rdds = []
            for index, rdd in enumerate(rdds):
                values = rdd[1].split("\n")
                new_value = {}
                for x in values:
                    y = json.loads(x)
                    new_value["%s:%s" % (y['columnFamily'], y['qualifier'])] = y['value']
                new_rdd = {}
                for column, v in catelog['columns'].items():
                    real_v = v['cf'] == 'rowkey' and rdd[0] or new_value.get("%s:%s" % (v['cf'], v['col']), None)
                    new_rdd[column] = value_type_transfer(real_v, v['type'])
                new_rdds.append(new_rdd)
            return new_rdds

        rdds = hbase_rdd.mapPartitions(hrdd_to_rdd)

        if cached:
            rdds.cache()
            self.cache_rdd[table] = rdds

        df = self.sqlContext.createDataFrame(rdds, self.catelog_to_schema(catelog))
        if catelog.get('domain', ''):
            df = df.filter(catelog['domain'])
        return df

    @staticmethod
    def catelog_to_schema(catelog):
        columns_dict = catelog.get('columns')
        columns = columns_dict.keys()
        structtypelist = [StructField(x, TYPE_DICT.get(columns_dict[x]['type'], StringType()), True) for x in columns]
        schema = StructType(structtypelist)
        return schema


@exception(logger)
def execute_load_data():
    load_data_to_hbase = LoadDataToHbase()
    logger.info("*" * 30 + "START TO INSERT DATA TO HBASE !!" + "*" * 30)

    # 导入User的基本信息
    # user_src = sqlcontext.sql("select * from new_type.user where time ='{time}' limit 100 ".format(time=TIME))
    # execute_func(load_data_to_hbase.insert_user_info_to_hbase, user_src, "insert Basic_Info !")

    # 导入Region信息
    # region_src = sqlcontext.sql("select * from  new_type.region_info where time ='{time}' ".format(time=TIME))
    # execute_func(load_data_to_hbase.insert_region_info_to_hbase, region_src, "insert Region_Info !")

    # 导入联系人信息
    # contact_src = sqlcontext.sql("select * from new_type.contact where time ='{time}' ".format(time=TIME))
    # execute_func(load_data_to_hbase.insert_contact_to_hbase, contact_src, "insert Contact! ")

    # 导入message
    # message_src = sqlcontext.sql("select mid,content,platform_msg_id ,"
    #                              "partyuid,useruid,timesent,media ,direction,msgtype,"
    #                              "platform from new_type.message where time ='{time}' ".format(time=TIME))
    # execute_func(load_data_to_hbase.insert_message_to_hbase, message_src, "insert message! ")

    # 导入group_msg
    group_msg_src = sqlcontext.sql("select * from new_type.group_message where time ='{TIME}'".format(time=TIME))
    execute_func(load_data_to_hbase.insert_groupmsg_to_hbase, group_msg_src, "insert groupmsg !")

    # 导入count 信息


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sqlcontext = HiveContext(sc)
    shb = SparkHbaseConn(sc, sqlcontext)
    sqlcontext.registerFunction("nvl", lambda x, y: x if x is not None else y)
    execute_load_data()
