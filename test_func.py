#!/usr/bin/env python
# coding: utf-8
# @Time    : 2018/8/22 11:46
# @Author  : zeng yu 
# @Site    : 
# @File    : test_func.py
# @Software: PyCharm

import json

import happybase

import transformData
from ConnectionUtil import HbaseConnect
from InsertData import SparkHbaseConn
from util import get_catlog
from pyspark.sql.functions import *
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType
from pyspark.sql import DataFrame

from pyspark import SparkConf, SparkContext, HiveContext, RDD


def test_substract():
    # Return each value in self that is not contained in other.
    a = sc.parallelize([("a", 1), ("b", 1), ("c", 2)])
    b = sc.parallelize([("a", 1), ("b", 2), ("c", 2)])
    # c = a.subtract(b)
    # print c.take(10)

    d = a.subtractByKey(b)
    print d.take(10)
    pass


def test_save_to_hbase():
    tests = SparkHbaseConn(sc, hc)
    catelog = {"table": {"namespace": "default", "name": "USER_TEST"}, "rowkey": "id", "domain": "",
               "columns": {"id": {"cf": "rowkey", "col": "key", "type": "string"},
                           "phone": {"cf": "INFO", "col": "PHONE", "type": "string"},
                           "mail": {"cf": "INFO", "col": "MAIL", "type": "string"},
                           "name": {"cf": "INFO", "col": "NAME", "type": "string"},
                           "platform": {"cf": "INFO", "col": "PLATFORM", "type": "string"}, }}

    df = hc.createDataFrame([{'id': '1', 'phone': json.dumps({"8613454546331": {"platform": ['tango'], "score": 1}}),
                              'mail': json.dumps({"qwerty@qq.com": {"platform": ['tango'], "score": 1}}),
                              'name': json.dumps(
                                  {"zy": {"platform": [{'source_id': '2', 'platform': 'tango'}], "score": 1}}),
                              'platform': json.dumps({'tango': {
                                  "platform": [{"password": "abc", "salt": "123", "reg_time": "20180102"}]}})},

                             {'id': '2', 'phone': json.dumps({"86124464521": {"platform": ['tango'], "score": 1}}),
                              'mail': json.dumps({"asdrty@qq.com": {"platform": ['tango'], "score": 1}}),
                              'name': json.dumps(
                                  {"naeada": {"platform": [{'source_id': '1', 'platform': 'tango'}], "score": 1}}),
                              'platform': json.dumps(
                                  {'tango': {"password": "sadd", "salt": "22", "reg_time": "20180814"}})}])
    # 'region': json.dumps([{"country": "中国", "province": "四川", "city": "内江"}])}])
    # df.show(1,False)
    tests.save_df_to_hbase(df, catelog)


def test_json_hbase():
    value = json.dumps([{"name": "xiaomi", "source_id": "Unknown"}, {"name": "huawei", "source_id": "apple"}])

    j_ob = json.dumps(value)

    import happybase
    conn = happybase.Connection(host="10.200.216.131", port=9090)
    table = conn.table("test")
    a = table.row("1")
    for x in a.iterkeys():
        print x, json.loads(a.get(x))


def test_join_func():
    df1 = sc.parallelize([{"id": 123, "name": "zy"}, {"id": 234, "name": "qwer"}, {"id": 2333, "name": "dasd"}])
    df2 = sc.parallelize([{"id": 123, "address": "neijiang"}, {"id": 234, "address": "xian"}])
    df1 = hc.createDataFrame(df1)
    df2 = hc.createDataFrame(df2)
    a = df1.join(df2, df1.id == df2.id, "left")
    a.show(10)


def test_Row():
    r = Row(uid="1", phone=123).asDict()
    r.iterkeys()


def test_groupby():
    a = sc.parallelize(
        [{"name": "abc", "phone": 123, "platform": "tango"}, {"name": "abc", "phone": 123, "platform": "hike"}])
    a = hc.createDataFrame(a)

    b = a.rdd.groupBy(lambda x: x.name)

    def func(x):
        k = x[0]
        v_l = x[1]
        phones = [(x.phone, x.platform) for x in v_l]

        return (k, phones)

    print b.map(func).collect()


def test_insert_normal_info(insert_data=DataFrame):
    shb = SparkHbaseConn(sc, hc)
    catelog = {"table": {"namespace": "default", "name": "USER_TEST"}, "rowkey": "id", "domain": "",
               "columns": {"userid": {"cf": "rowkey", "col": "key", "type": "string"},
                           "mail": {"cf": "INFO", "col": "MAIL", "type": "string"},
                           "name": {"cf": "INFO", "col": "NAME", "type": "string"},
                           "phone": {"cf": "INFO", "col": "PHONE", "type": "string"},
                           "platform": {"cf": "INFO", "col": "PLATFORM", "type": "string"},
                           "reg_time": {"cf": "INFO", "col": "REG_TIME", "type": "string"},
                           "password": {"cf": "INFO", "col": "PASSWORD", "type": "string"},
                           "salt": {"cf": "INFO", "col": "SALT", "type": "string"},
                           "gender": {"cf": "INFO", "col": "GENDER", "type": "string"},
                           "birthday": {"cf": "INFO", "col": "BIRTHDAY", "type": "string"},
                           "address": {"cf": "INFO", "col": "ADDRESS", "type": "string"},
                           "area": {"cf": "INFO", "col": "AREA", "type": "string"},
                           "profession": {"cf": "INFO", "col": "PROFESSION", "type": "string"},
                           "card": {"cf": "INFO", "col": "CARD", "type": "string"},
                           "facebookid": {"cf": "INFO", "col": "FACEBOOKID", "type": "string"},
                           "ip": {"cf": "INFO", "col": "IP", "type": "string"},
                           "lives": {"cf": "INFO", "col": "LIVES", "type": "string"},
                           "race": {"cf": "INFO", "col": "RACE", "type": "string"},
                           "status": {"cf": "INFO", "col": "STATUS", "type": "string"}}}

    def func(x):
        # 标准格式 如 Name
        # {"name":
        #   {"zy":{"platform":[tango-123,hike-456],"score":2}}
        #   {"tl":{"platform":[tango-abc],"score":1}}
        # }
        # Phone
        # {"phone":
        # { "86123456":{"platform":["tango"],"score":1}
        #   }
        # }
        result = {}
        userid = x[0]
        values = x[1]  # Iter or Row[]
        row_dict = list(values)[0].asDict()

        special_cols = ["userid", 'name', 'platform', 'salt', 'password', 'reg_time', 'name_source']
        for key in row_dict.iterkeys():
            if key not in special_cols:
                items = [(row[key], row["platform"]) for row in values]
                item_dict = {}
                for item, platform in items:
                    if item_dict.get(item) is None:
                        item_dict[item] = {"platform": [platform], "score": 1}
                    else:
                        a = item_dict[item]["platform"]
                        if not a.__contains__(platform):
                            a.append(platform)
                            item_dict[item] = {"platform": a, "score": a.__len__()}
                result[key] = item_dict
            elif key == "name":
                names = [(row.name, row.name_source, row.platform) for row in values]
                names = set(names)
                name_dict = {}
                for name, source, platform in names:
                    if name_dict.get(name) is None:
                        name_dict[name] = {"platform": [{"source_id": source, "platform": platform}], "score": 1}
                    else:
                        a = name_dict[name]["platform"]
                        a.append({"source_id": source, "platform": platform})
                        name_dict[name] = {"platform": a, "score": a.__len__()}
                result[key] = name_dict

            elif key == 'platform':
                platform_infos = [(row["platform"], row["salt"], row["password"], row['reg_time']) for row in values]
                platform_infos = set(platform_infos)
                item_dict = {}
                for platform, salt, password, reg_time in platform_infos:
                    if item_dict.get(platform) is None:
                        item_dict[platform] = {"platform": [{"salt": salt, "password": password, "reg_time": reg_time}],
                                               "score": 1}
                    else:
                        a = item_dict[platform]["platform"]
                        a.append({"salt": salt, "password": password, "reg_time": reg_time})
                        item_dict[platform] = {"platform": a, "score": a.__len__()}
                result[key] = item_dict

                pass
        result["userid"] = userid
        return result

    result = insert_data.rdd.groupBy(lambda x: x.userid).map(func)
    print result.take(10)  # shb.save_df_to_hbase(insert_data, catelog)


def test_merge_hbase_data():
    a = sc.parallelize([("1", {"phone": {"861325454": {"platform": ["tango"], "score": 1},
                                         "8613454546331": {"platform": ["hike"], "score": 1}},
                               "name": {"zy": {"platform": [{"source_id": "dasd", "platform": "hike"}], "score": 1}},
                               "mail": {"abc@qq.com": {"platform": ["hike"], "score": 1}}, "platform": {
            "hikeee": {"platform": [{"salt": 123, "password": "dasd", "reg_time": "dasdas"}], "score": 1}}})])
    title = ["userid", "name", "phone", "mail", "platform"]
    User_model = Row(*title)
    struct_fields = [StructField(x, StringType(), True) for x in title]
    user_schema = StructType(struct_fields)

    def merge(iter):
        hbase_conn_pool = HbaseConnect(host="10.200.11.35", port=19090).get_hbase_connect_pool()
        with hbase_conn_pool.connection() as conn:
            table = conn.table('USER_TEST')
            for row in iter:
                result = {}

                uid, local_all_value = row
                result["userid"] = uid
                hbase_all_value = table.row(uid)  # 将json 字符串转换成python对象
                if hbase_all_value:
                    for key in local_all_value.iterkeys():
                        # 比如 key 为 name
                        local_value = local_all_value[
                            key]  # {zy:{'platform':[{'platform':'tango','source_id':'qwz'}],'score':1}}
                        hbase_value = json.loads(hbase_all_value["INFO:%s" % str(key).upper()])  # hbase 的结构同上
                        new_value = {}  # 合并后的name_dict
                        new_value.update(hbase_value)  # 先存入hbase的value

                        for l_key in local_value.iterkeys():
                            if new_value.has_key(l_key):
                                # 如果Hbase 存在相同的值，则需对其进行合并
                                l_platforms = local_value.get(l_key).get("platform")
                                h_platforms = new_value[l_key].get("platform")
                                # h_platforms 是一个LIST 如[{'platform':'tango','source_id':'qwz'},{'platform':'tango','source_id':'sadas'}]
                                for p in l_platforms:
                                    if not h_platforms.__contains__(p):
                                        h_platforms.append(p)
                                new_value[l_key] = {"platform": h_platforms, "score": h_platforms.__len__()}
                            else:
                                new_value[l_key] = local_value[l_key]  # 无需合并
                        result[key] = new_value

                else:
                    result.update(local_all_value)

                data = [json.dumps(result.get(t)) for t in title]
                data[0] = json.loads(data[0])  # userid 无需转换成json对象
                yield User_model(*data)

    x = a.mapPartitions(merge)
    df = hc.createDataFrame(x, schema=user_schema)
    df.show(10, False)
    # shc = spark_hbase_con(sc, hc)
    # shc.save_df_to_hbase(df, get_catlog(*title, table_name="USER_TEST", rowkey="userid", family="INFO"))


def test_hbaseinsert():
    hbase_conf = {  "hbase.zookeeper.quorum":"hadoop001,hadoop002,hadoop003",
                    "hbase.mapred.outputtable":"USER_TEST",
                    "mapreduce.outputformat.class":"org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
                    "mapreduce.outputformat.key.class":"org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                    "mapreduce.outputformat.value.class": "org.apache.hadoop.io.Writable"

                  }
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    rawData = ['3,INFO,name,Rongcheng', '4,INFO,phone,123456']
    sc.parallelize(rawData).map(lambda x: (x[0], x.split(','))).saveAsNewAPIHadoopDataset(conf=hbase_conf,
                                                                                          keyConverter=keyConv,
                                                                                          valueConverter=valueConv)


if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    hc = HiveContext(sc)
    # test_join_func()
    # test_Row()
    # test_groupby()  # data = ["aaafgftango", "asdasfgftango", "aaafgfhike"]  # ns_dict = [a.split('fgf') for a in data]  # kv_dict = {}  # for k, v in ns_dict:  #     if kv_dict.get(k) is None:  #         kv_dict[k] = {"platform": [v], "score": 1}  #     else:  #         a = kv_dict.get(k).get("platform")  #         a.append(v)  #         kv_dict[k] = {"platform": a, "score": a.__len__()}  # print kv_dict
    # insert_data = sc.parallelize([{"userid": "uid1", "name": "zy", "mail": "123@qq.com", "phone": "8612123123",
    #                                "platform": "tango", "salt": "asd", "password": "dasdasd", "name_source": "tc",
    #                                "reg_time": "2018"},
    #                               {"userid": "uid1", "name": "zy22", "mail": "1234@qq.com", "phone": "861231231233",
    #                                "platform": "tango", "salt": "asdasf", "password": "as2qwdasdasd",
    #                                "name_source": "tl", "reg_time": "2018"},
    #                               {"userid": "uid2", "name": "qww", "mail": "1ww34@qq.com", "phone": "8615631231233",
    #                                "platform": "tango", "salt": "aas", "password": "sddf", "name_source": "zyb",
    #                                "reg_time": "2018"} ])
    # df = hc.createDataFrame(insert_data)
    # test_merge_hbase_data()
    # test_hbaseinsert()
    table = happybase.Connection(host="10.200.11.35", port=19090).table("USER_TEST")
    a = table.row("0074cffb2e2fc36264fb6f7abf21abec-viber")
    for key in json.loads(a.get("INFO:NAME")).iterkeys():

        print key