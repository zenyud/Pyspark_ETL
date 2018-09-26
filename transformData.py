#!/usr/bin/env python
# coding: utf-8
# @Time    : 2018/8/15 17:45
# @Author  : zeng yu 
# @Site    : 
# @File    : transformData.py
# @Software: PyCharm
import datetime
import hashlib
import logging
import time
import uuid
from abc import abstractmethod

from getRegion import *
from pyspark import SparkConf, SparkContext, HiveContext, Row
from pyspark.sql import DataFrame
from pyspark.sql.types import *

now = datetime.datetime.now().date()
date = now - datetime.timedelta(days=1)
# TIME = str(now).replace("-", "")
# DATE = str(date).replace("-", "")
DATE = '20180605'
TIME = '20180606'

USER_TITLE = ["rowkey", "userid", "name", "name_source", "mail", "phone", "platform", "reg_time", "update_time",
              "password", "salt", "gender", "birthday", "address", "area", "profession", "card", "facebookid", "ip",
              "lives", "race", "status", "is_user"]  # user 表的字段名列表


def get_user_schema(t=[]):
    u_table_schema = StructType()
    for x in t:
        if x != 'is_user':
            u_table_schema.add(StructField("%s" % x, StringType(), True))
        else:
            u_table_schema.add(StructField("%s" % x, IntegerType(), True))
    return u_table_schema


def getLogger(filename, filepath, filemode):
    lg = logging.getLogger()
    logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d %A %H:%M:%S', filename=filename, filemode=filemode)
    fh = logging.FileHandler(filepath)
    formatter = logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    lg.addHandler(fh)
    return lg


def exception(logger_obj):
    """
    程序运行日志装饰器
    @param logger: The logging object
    """

    def decorator(func):

        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                # log the exception
                err = "There was an exception in  "
                err += func.__name__
                logger_obj.exception(err)  # re-raise the exception

        return wrapper

    return decorator


logger = getLogger("{0}.log".format(TIME), "{0}.log".format(TIME), "a+")

USER_TABLE_SCHEMA = get_user_schema(USER_TITLE)  # 创建UserTable所需的schema

HIVE_USER_TABLE_MODEL = Row(*USER_TITLE)  # user Row 模型


def user_model(**kwargs):
    data = [kwargs.get(v) for v in USER_TITLE]
    return HIVE_USER_TABLE_MODEL(*data)


def _get_data(table_name, partition_name):
    return hc.sql("select * from {tablename} where {partition_name} = '{date}' ".format(tablename=table_name, date=DATE,
                                                                                        partition_name=partition_name))


def get_md5_str(userid):
    md = hashlib.md5()
    md.update(userid)
    return md.hexdigest()


def execute_func(func, rdd, rdd_name):
    if rdd:
        logger.info('start clear %s !' % rdd_name)
        func(rdd)
        logger.info("clear %s is done" % rdd_name)
    else:
        logger.warn('%s  rdd is Empty ! ' % rdd_name)
        pass


class TransformData(object):
    """
    Rowkey 设计为userid 的MD5 Hash值+平台名
    """

    def __init__(self, platform):
        self.platform = platform
        pass

    @abstractmethod
    def clear_user(self, user_src):
        """
        :param
        :return:
        user_src_cleaned :userid 平台唯一id,name,mail,phone,platform ,reg_time ,
        password,salt, gender, birthday ,address,area,profession,card,facebookid,ip,lives,race,status
        """
        pass

    @abstractmethod
    def clear_contact(self, contact_src):
        pass

    @abstractmethod
    def clear_message(self, message_src):
        pass

    @abstractmethod
    def clear_group_message(self, groupmessage_src):
        pass

    @staticmethod
    def generate_region(t, p):
        """
        :return: 根据电话号码 生成归属地
        """
        PLATFORM = TransformTango.PLATFORM
        input = hc.sql("select rowkey,phone,platform from new_type.user "
                       "where  time ='{time}' and platform ='{platform}' and phone!='null' and phone is not null  "
                       "group by rowkey,phone,platform ".format(time=t, platform=p))

        def func(row):
            region = phoneArea.getPhoneAreaWithJsonNew(row.phone, phone_json.value)
            country, province, city = region.get("country"), region.get('province'), region.get('city')
            return Row(rowkey=row.rowkey, country=country.decode('utf8') if country else 'Unknown',
                       province=province.decode('utf8') if province else 'Unknown',
                       city=city.decode('utf8') if city else 'Unknown', code=region.get("code"), phone=row.phone)

        tmp = input.map(func)
        tmp = hc.createDataFrame(tmp)
        tmp.registerTempTable("tmp_region")
        hc.sql("insert overwrite table new_type.region_info partition(time='{time}',platform='{platform}') "
               "select rowkey,country,province,city,code,phone from tmp_region".format(time=t, platform=p))
        hc.dropTempTable('tmp_region ')

    @staticmethod
    def insert_contact(tablename, TIME, PLATFORM):
        hc.sql("insert into table new_type.contact partition(time='{time}', platform='{platform}') "
               "select u_uid,c_uid,name from {tablename}".format(time=TIME, platform=PLATFORM, tablename=tablename))

    @staticmethod
    def insert_user(tablename, TIME, PLATFORM):
        hc.sql("insert into table new_type.user partition(time='{time}',platform='{platform}')"
               "select rowkey, userid ,name,name_source, mail,"
               "phone ,reg_time,update_time,password,salt, gender, birthday "
               ",address,area,profession,card,facebookid,ip,lives,race,status,is_user   "
               "from {tablename} ".format(time=TIME, platform=PLATFORM, tablename=tablename))


class TransformTango(TransformData):
    PLATFORM = "tango"

    def __init__(self, platform):
        super(TransformTango, self).__init__(platform)
        self.platform = platform

    @classmethod
    def clear_user(cls, user_src):
        """user_src
        :param  输入数据 tango_user 原始表
        :return:
        user_src_cleaned :userid 平台唯一id,name,mail,phone,platform ,reg_time ,
        password,salt, gender, birthday ,address,area,profession,card,facebookid,ip,lives,race,status
        """
        PLATFORM = TransformTango.PLATFORM

        def clean(row):
            rowkey = get_md5_str(row.tango_id) + '-tango'
            name = deal_name(row.name)
            phone = deal_phone(row.phone)
            mail = deal_mail(row.mail)

            return user_model(rowkey=rowkey, userid=row.tango_id, name=name, name_source="Unknown", phone=phone,
                              mail=mail, password=row.password, update_time=row.update_time, platform=PLATFORM,
                              is_user=1)

        tmp = user_src.rdd.map(clean)

        df = hc.createDataFrame(tmp, schema=USER_TABLE_SCHEMA)

        df.registerTempTable("user_tmp")
        hc.sql("insert overwrite table new_type.user partition(time='{time}',platform='{platform}')"
               "select rowkey, userid ,name,name_source, mail,phone ,reg_time,update_time,password,salt, gender, birthday "
               ",address,area,profession,card,facebookid,ip,lives,race,status,is_user   "
               "from user_tmp ".format(time=TIME, platform=PLATFORM))
        hc.dropTempTable("user_tmp")

    @classmethod
    def clear_contact(cls, contact_src):
        PLATFORM = TransformTango.PLATFORM
        tmp = contact_src.rdd.map(lambda x: Row(u_uid=get_md5_str(x.userid) + '-' + PLATFORM,
                                                c_uid=get_md5_str(x.tango_to_id) + '-' + PLATFORM,
                                                name=deal_name(x.name), platform=PLATFORM))
        df = hc.createDataFrame(tmp)

        df.registerTempTable('contact_tmp')
        TransformData.insert_contact('contact_tmp', TIME, PLATFORM)
        hc.dropTempTable('contact_tmp')

    @classmethod
    def clear_message(cls, message_src):
        def func(row):
            user = get_md5_str(row.useruid) + '-tango'
            party = get_md5_str(row.partyuid) + '-tango'
            mid = '-'.join([useruid, str(row.timesent), row.messagetoken])
            return Row(mid=mid, useruid=row.useruid, partyuid=row.partyuid, user=user, party=party, platform='tango',
                       platform_msg_id=row.messagetoken, content=row.mtext, media=row.media, direction=row.direction,
                       timesent=str(row.timesent), msgtype=row.msgtype, name=get_name(row.mtext))

        tmp = message_src.rdd.map(func)
        df = hc.createDataFrame(tmp)
        df.registerTempTable("message_tmp")

        tmp_distinct = hc.sql(
            "select collect_set(mid)[0] mid,content,platform,partyuid,useruid,user,party, timesent,media,direction,msgtype,name "
            "from message_tmp "
            "group by  content,platform, partyuid, useruid, user, party, media,direction,msgtype,name,timesent ")
        hc.dropTempTable("message_tmp")
        tmp_distinct.registerTempTable("message_tmp2")
        tmp_distinct.cache()
        hc.sql("""insert overwrite table new_type.message partition(time ='{time}',platform='{platform}')
                     select mid,content,split(mid,'-')[3] platform_msg_id,partyuid,useruid,party, user,
                     timesent,media,direction,msgtype,name
                     from message_tmp2 """.format(time=TIME, platform="tango"))

        # 将关系存入contact 表
        hc.sql("insert into table new_type.contact partition(time ='{time}',platform='{platform}')  "
               "select party,user,name "
               "from  message_tmp2 group by user,party,name ".format(time=TIME, platform='tango_message'))

        # name 存入user表
        message_name = tmp_distinct.filter("name!='null' ").map(
            lambda x: user_model(rowkey=x.user, userid=x.useruid, name=x.name, name_source=x.party,
                                 platform='tango_message', is_user=1)).distinct()
        message_name = hc.createDataFrame(message_name, schema=USER_TABLE_SCHEMA)
        message_name.registerTempTable("message_name")
        TransformData.insert_user('message_name', TIME, 'tango_message')


    @classmethod
    def clear_group_message(cls, groupmsg_src):
        def func(x):
            mid = '-'.join(['tango', x.groupid, str(x.timesent), x.messagetoken])
            useruid = get_md5_str(x.userid) + '-tango'
            GROUP_MESSAGE = Row("mid", "content", 'gid', 'platform_msg_id', 'useruid', 'timesent', 'media', 'msgtype')

            return GROUP_MESSAGE(mid, x.mtext, x.groupid, x.messagetoken, useruid, x.timesent, x.media, x.msgtype)

        tmp = groupmsg_src.map(func)
        df = hc.createDataFrame(tmp)
        df.registerTempTable("tmp_groupmsg")
        tmp_distinct = hc.sql(
            "select collect_set(mid)[0] mid, content,gid, useruid, timesent, media, msgtype from tmp_groupmsg "
            "group by content, gid, useruid, timesent, media, msgtype")
        tmp_distinct.registerTempTable("tmp_groupmsg2")
        hc.sql("insert overwrite table new_type.groupmessage partition(time='{time}',platform='{platform}') select "
               "mid,content,gid, split(mid,'-')[3] platform_msg_id, useruid, timesent, media, msgtype "
               "from tmp_groupmsg".format(time=TIME, platform='tango'))

    @classmethod
    def generate_region(cls, t, p):
        TransformData.generate_region(t, p)

    @classmethod
    def clear_groupmember(cls, groupmember_src=DataFrame):
        def func(row):
            groupuid = 'tango-' + row.groupid
            uuid = get_md5_str(row.uid) + '-tango'
            return Row(groupuid=groupuid, groupid=row.groupid, uid=row.uid, uuid=row.uuid, role=row.role, time=TIME,
                       platform='tango')

        tmp = groupmember_src.map(func)
        groupmember = hc.createDataFrame(tmp)
        groupmember.registerTempTable("group_member")
        hc.sql("insert into table new_type.group_member partition(time,platform) "
               "select groupuid,groupid,uid,role,uuid,time,platform from group_member)")
        hc.dropTempTable("group_member")

    @classmethod
    def clear_groupinfo(cls, groupinfo_src):
        def func(row):
            groupuid = 'tango-' + row.groupid
            createruuid = get_md5_str(row.createruid) + '-tango'
            return Row(groupuid=groupuid, groupname=row.groupname, createruuid=createruuid, created=row.created,
                       grouptype=row.grouptype, groupid=row.groupid, time=TIME, platform='tango')

        tmp = groupinfo_src.map(func)
        groupinfo = hc.createDataFrame(tmp)
        groupinfo.registerTempTable("group_info")
        hc.sql("insert into table new_type.group_info partition(time,platform) select "
               "groupuid,groupid,groupname,createruid,"
               "created,grouptype,createruuid,time,platform from group_info")
        hc.dropTempTable("group_info")

    @classmethod
    @exception(logger)
    def execute_transform(cls):
        """
        执行tango 清洗
        """
        logger.info("*" * 50)
        logger.info("START TO EXECUTE Tango_Transform !!")

        user_src = _get_data("tango.tango_user_src", "data_time")
        execute_func(TransformTango.clear_user, user_src, 'user_rdd')

        contact_src = _get_data("tango.tango_contact_src", "data_time")
        execute_func(TransformTango.clear_contact, contact_src, 'contact_rdd')

        msg_src = _get_data("tango.tango_message_src", 'data_time')
        execute_func(TransformTango.clear_message, msg_src, 'message_rdd')

        group_msg = _get_data('tango.tango_groupmessage_src', 'data_time')
        execute_func(TransformTango.clear_group_message, group_msg, 'groupmsg_rdd')

        group_member = _get_data('tango.tango_groupmember_src', 'data_time')
        execute_func(TransformTango.clear_groupmember, group_member, 'group_member_rdd ')

        group_info = _get_data('tango.tango_groupinfo_src', 'data_time')
        execute_func(TransformTango.clear_groupinfo, group_info, 'group_info_rdd ')

        logger.info('start genarate info !')
        TransformTango.generate_region(TIME, 'tango')
        logger.info('genarate info done  ! ')

        logger.info('FINISH TO EXECUTE Tango_Transform !! ')
        logger.info('*' * 50)


class TransformNimbuzz(TransformData):
    PLATFORM = 'nimbuzz'

    def __init__(self, platform):
        super(TransformNimbuzz, self).__init__(platform)

    @classmethod
    def clear_user(cls, user_src):
        platform = TransformNimbuzz.PLATFORM

        def clean(row):
            rowkey = get_md5_str(str(row.user_id)) + '-nimbuzz'
            name = deal_name(row.user_name)
            phone = deal_phone(row.phone)
            mail = deal_mail(row.mail)

            return user_model(rowkey=rowkey, userid=row.user_id, name=name, name_source="Unknown", phone=phone,
                              mail=mail, update_time=row.update_time, platform=platform, status=row.status,
                              gender=deal_gender(row.gender), birthday=row.birthday, reg_time=row.register,
                              lives=row.lives, is_user=1)

        tmp = user_src.rdd.map(clean)

        df = hc.createDataFrame(tmp, schema=USER_TABLE_SCHEMA)
        df.registerTempTable("user_tmp")
        TransformData.insert_user('user_tmp', TIME, platform)
        hc.dropTempTable('user_tmp')

    @classmethod
    def clear_contact(cls, contact_src):
        platform = TransformNimbuzz.PLATFORM
        tmp = contact_src.rdd.map(lambda x: Row(u_uid=get_md5_str(str(x.user_own_id)) + '-' + platform,
                                                c_uid=get_md5_str(str(x.user_to_id)) + '-' + platform, name='null'))
        df = hc.createDataFrame(tmp)

        df.registerTempTable("contact_tmp")
        TransformData.insert_contact('contact_tmp', TIME, platform)
        hc.dropTempTable('contact_tmp')
        pass

    @classmethod
    def generate_region(cls, t='', p=''):
        TransformData.generate_region(t, p)

    def clear_message(self, message_src):
        pass

    def clear_group_message(self, **kwargs):
        pass

    def get_data(self, get_data_sql):
        pass

    @classmethod
    @exception(logger)
    def execute_transform(cls):
        """
            执行nimbuzz的清洗
        :return:
        """
        logger.info("*" * 50)
        logger.info('START TO EXECUTE Nimbuzz_Transform !!')

        user_src = _get_data('nimbuzz.nimbuzz_user_new', 'data_time')
        execute_func(TransformNimbuzz.clear_user, user_src, 'user_rdd')

        contact_src = _get_data('nimbuzz.nimbuzz_contact_src', 'data_time')
        execute_func(TransformNimbuzz.clear_contact, contact_src, 'contact_rdd')

        logger.info('start generate region !')
        cls.generate_region(t=TIME, p='nimbuzz')
        logger.info('generate region done ! ')

        logger.info('FINISH TO EXECUTE Nimbuzz_Transform !! ')
        logger.info('*' * 50)


class TransformHike(TransformData):
    PLATFORM = 'hike'

    @classmethod
    def clear_user(cls, user_src):
        PLATFORM = TransformHike.PLATFORM

        def clean(row):
            rowkey = get_md5_str(row.uid) + '-hike'
            name = deal_name(row.name)
            phone = deal_phone(row.phone)
            mail = deal_mail(row.mail)

            return user_model(rowkey=rowkey, userid=row.uid, name=name, name_source="Unknown", phone=phone, mail=mail,
                              birthday=deal_birth(row.birth), reg_time=row.reg_time, platform=PLATFORM,
                              update_time=row.update_time, gender=deal_gender(row.gender), is_user=1)

        tmp = user_src.rdd.map(clean)

        df = hc.createDataFrame(tmp, schema=USER_TABLE_SCHEMA)

        df.registerTempTable("user_tmp")
        TransformData.insert_user('user_tmp', TIME, PLATFORM)
        hc.dropTempTable("user_tmp")

    @classmethod
    def clear_contact(cls, contact_src):
        def func(row):
            """
            :param row: Hike 联系人表
            :return:
            """
            u_phone, c_phone = deal_phone(row.u_phone), deal_phone(row.c_phone)
            userid = row.u_uid if row.u_is == 1 else u_phone
            contactid = row.c_uid if row.c_is == 1 else c_phone
            u_uid = get_md5_str(userid) + '-hike'
            c_uid = get_md5_str(contactid) + '-hike'
            u_is = row.u_is
            c_is = row.c_is
            name = deal_name(row.c_name)
            return Row(u_uid=u_uid, c_uid=c_uid, userid=userid, contactid=contactid, u_is=u_is, c_is=c_is, name=name,
                       u_phone=u_phone, c_phone=c_phone)

        tmp = contact_src.limit(100).map(func)
        tmp_contact = hc.createDataFrame(tmp)
        tmp_contact.cache()
        tmp_contact.registerTempTable('tmp_contact')
        # 插入联系人表
        TransformData.insert_contact('tmp_contact', TIME, 'contact')
        hc.dropTempTable('tmp_contact')
        # 基础信息插入user表
        uid_tmp = tmp_contact.map(lambda x: user_model(rowkey=x.u_uid, userid=x.userid, phone=x.u_phone, is_user=x.u_is,
                                                       platform='hike')).distinct()
        c_uid_tmp = tmp_contact.map(
            lambda x: user_model(rowkey=x.c_uid, userid=x.contactid, phone=x.c_phone, is_user=x.c_is, name=x.name,
                                 name_source=x.u_uid, platform='hike')).distinct()
        uid_tmp = hc.createDataFrame(uid_tmp, USER_TABLE_SCHEMA)
        c_uid_tmp = hc.createDataFrame(c_uid_tmp, USER_TABLE_SCHEMA)
        uid_tmp.registerTempTable('u_uid_tmp')
        c_uid_tmp.registerTempTable('c_uid_tmp')
        TransformData.insert_user('u_uid_tmp', TIME, 'hike')
        TransformData.insert_user('c_uid_tmp', TIME, 'hike')
        hc.dropTempTable('u_uid_tmp')
        hc.dropTempTable('c_uid_tmp')
        tmp_contact.unpersist()

    def clear_platform(self, platform_src):
        pass

    @classmethod
    def generate_region(cls, t, p):
        TransformData.generate_region(t, p)
        pass

    def clear_message(self, message_src):
        pass

    def clear_group_message(self, **kwargs):
        pass

    @classmethod
    @exception(logger)
    def execute_transform(cls):
        logger.info("*" * 50)
        logger.info('START TO EXECUTE Hike_Transform !!')

        user_src = _get_data('hike.user_src', 'date')
        execute_func(TransformHike.clear_user, user_src, 'user_rdd')

        contact_src = _get_data('hike.contact_src', 'date')
        execute_func(TransformHike.clear_contact, contact_src, 'contact_rdd')

        logger.info('start generate region !')
        cls.generate_region(t=TIME, p='hike')
        logger.info('generate region done ! ')

        logger.info('FINISH TO EXECUTE Hike_Transform !! ')
        logger.info('*' * 50)

        pass


class TransformViber(TransformData):
    @classmethod
    def clear_user(cls, user_src):
        def func(row):
            phone = deal_phone(row.phone)
            userid = phone
            rowkey = get_md5_str(phone) + '-viber'
            update_time = row.update_time
            return user_model(rowkey=rowkey, userid=userid, phone=phone, update_time=update_time, is_user=1)

        tmp = user_src.map(func)
        tmp = hc.createDataFrame(tmp, schema=USER_TABLE_SCHEMA)
        tmp.registerTempTable('user_tmp')
        TransformData.insert_user('user_tmp', TIME, 'viber')
        hc.dropTempTable('user_tmp')

    @classmethod
    def clear_contact(cls, contact_src):
        def func(row):
            u_phone = deal_phone(row.userid)
            c_phone = deal_phone(row.tango_to_id)
            u_uid = get_md5_str(u_phone) + '-viber'
            c_uid = get_md5_str(c_phone) + '-viber'
            name = deal_name(row.name)
            return Row(u_uid=u_uid, c_uid=c_uid, u_phone=u_phone, c_phone=c_phone, name=name)

        tmp = contact_src.map(func)
        tmp = hc.createDataFrame(tmp)
        tmp.cache()
        tmp.registerTempTable('tmp_contact')
        TransformData.insert_contact('tmp_contact', TIME, 'contact')
        hc.dropTempTable('tmp_contact')
        uid_tmp = tmp.map(lambda x: user_model(rowkey=x.u_uid, userid=x.u_phone, platform='viber', is_user=1,
                                               phone=x.u_phone)).distinct()
        contact_tmp = tmp.map(
            lambda x: user_model(rowkey=x.c_uid, userid=x.c_phone, platform='viber', is_user=0, phone=x.c_phone,
                                 name=x.name, name_source=x.u_uid)).distinct()
        uid_tmp = hc.createDataFrame(uid_tmp, USER_TABLE_SCHEMA)
        contact_tmp = hc.createDataFrame(contact_tmp, USER_TABLE_SCHEMA)
        uid_tmp.registerTempTable('uid_tmp')
        contact_tmp.registerTempTable('tmp_contact')
        TransformData.insert_user('uid_tmp', TIME, 'viber')
        TransformData.insert_user('tmp_contact', TIME, 'viber')
        hc.dropTempTable('uid_tmp')
        hc.dropTempTable('tmp_contact')

    @classmethod
    def generate_region(cls, t, p):
        TransformData.generate_region(t, p)

    def clear_message(self, message_src):
        pass

    def clear_group_message(self, **kwargs):
        pass

    @classmethod
    @exception(logger)
    def execute_transform(cls):
        logger.info("*" * 50)
        logger.info('START TO EXECUTE VIBER_Transfrom !!')

        user_src = _get_data('viber.viber_login_info', 'data_time')
        execute_func(TransformViber.clear_user, user_src, 'user_rdd')

        contact_src = _get_data('viber.viber2_contact_src', 'data_time')
        execute_func(TransformViber.clear_contact, contact_src, 'contact_rdd')

        logger.info('start generate region !')
        cls.generate_region(t=TIME, p='viber')
        logger.info('generate region done ! ')

        logger.info('FINISH TO EXECUTE VIBER_Transform !! ')
        logger.info('*' * 50)


def deal_phone(phone):
    """
    :param phone: 输入的电话号码
    :return: 去除不正常的电话号码
    """
    black_phone = "112233|789456123|147258369|987654|87654|123456|654321|54321|" \
                  "111|222|333|444|555|666|777|888|999|147852369|87654321|7654321|1478963|3456|45678|13579"
    if phone is None:
        return 'null'
    else:
        phone = re.sub("\s+", "", phone)
        phone = re.sub("\D", "", phone)

        if len(phone) < 7 or len(phone) > 16 or phone[0] == 0 or re.match(black_phone,
                                                                          phone) is not None or not phone.isdigit():
            return 'null'
        # try:
        #     result = phonenumbers.parse('+%s' % phone, None)
        # except NumberParseException as e:
        #     return 'null'
        # if phonenumbers.is_valid_number(result):
        #     return phone.strip()  # 使用phonenumbers库 验证号码是否合理
        else:
            # 该号码可能是未加国家码的号码,也有可能是错误号码
            return phone.strip()


def deal_name(name):
    reload(sys)
    sys.setdefaultencoding('utf-8')
    try:
        a = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+".decode("utf8"), "".decode("utf8"), name)
        if a == "":
            return 'null'
        else:
            return str(name).strip()
        pass
    except Exception:
        return 'null'


def time_plus(timesent):
    t_len = str(timesent).__len__()
    if t_len < 13:
        timesent = str(timesent) + "000"
    return timesent


def deal_mail(mail):
    reload(sys)
    sys.setdefaultencoding('utf-8')
    if mail is None:
        return 'null'
    elif not str(mail).__contains__("@"):
        return 'null'
    elif str(mail).__contains__("@"):
        mail_list = str(mail).split("@")
        if len(mail_list) != 2:
            return 'null'
        else:
            account, dns = str(mail).split("@")
            if str(account).__len__() < 4:
                return 'null'
            switch = {"gmail.com": "[a-z0-9.]{6,30}", "yahoo.com": "(a-z)[a-z0-9._]{4,32}"}
            re_str = switch.get(dns)

            if re_str is not None:
                if not re.match(re_str, account, re.I):
                    return 'null'
                else:
                    return re.sub("\s+", "", mail)
            else:
                return re.sub("\s+", "", mail)


def deal_birth(birth):
    if birth != 'null':
        try:
            a = re.sub("\"|{|}|\s+", "", birth)
            a = a.split(",")
            day = "1"
            month = "1"
            year = "0000"
            for x in a:
                c, b = x.split(":")
                if c == "day":
                    day = b
                if c == "month":
                    month = b
                if c == "year":
                    year = b

            date = year + "-" + month + "-" + day
            a = int(time.mktime(time.strptime(date, '%Y-%m-%d')))
            print str(a)
            return str(a)
        except Exception as ex:
            return 'null'
    else:
        return 'null'


def deal_platform(platforms):
    s = set()
    a = ""
    if platforms != "null":
        try:
            platforms = platforms.split("\x02")
            for p in platforms:
                platform = p.split("\t")[4]
                s.add(platform)

            for x in s:
                if a.__eq__(""):
                    a = x
                else:
                    a = a + "\t" + x
            return a
        except Exception as ex1:
            return 'null'
    else:
        return "null"


def self_nvl(c1, c2, c3):
    if c1 is not None:
        return c1
    else:
        if c2 is not None:
            return c2
        else:
            return c3


def self_uid():
    return uuid.uuid1().__str__().replace("-", "")


def get_name(content):
    # s = {"也登入 Tango", "is back on Tango", "也登录了 Tango","님이 Tango"}
    if re.search(r"也登入 Tango*|is back on Tango*|也登录了 Tango*|님이 Tango*", content):
        name = re.sub(r"也登入 Tango*|is back on Tango*|也登录了 Tango*", "", content).split(",")[0]
        return name
    else:
        return "null"


def self_filter(content):
    if re.search(r"也登入 Tango*|is back on Tango*|也登录了 Tango*", content):
        return "null"
    else:
        return content


def deal_gender(sex):
    switch = {"male": "true", "female": "false", "man": "true", "woman": "false", "fm": "false", "FM": "false",
              "M": "true", "m": "true", "f": "false", "F": "false"}
    sex = str(sex).lower()
    sex = switch.get(sex)
    if sex:
        return sex
    else:
        'null'


def message_count():
    def src_message_count():
        hc.sql("""INSERT overwrite TABLE new_type.message_count_tmp PARTITION (time,platform) SELECT
                             d.uuid,
                             d. USER,
                             d.count,
                             d.content,
                             d.timesent,
                             d.platform_msg_id,
                             d.partyuid,
                             d.party,
                             d.time,
                             d.platform
                         FROM
                             (
                                 SELECT
                                     a.uuid,
                                     a. USER,
                                     count,
                                     self_filter (b.content) content,
                                     a.time,
                                     b.platform_msg_id,
                                     b.partyuid,
                                     a.party,
                                     b.platform
                                 FROM
                                     (
                                         SELECT
                                             concat(USER, '-', party) uuid,
                                             count(1) count,
                                             USER,
                                             max(timesent) timesent,
                                             o.party
                                         FROM
                                             new_type.message o
                                         WHERE
                                             msgtype = 0
                                         GROUP BY
                                             USER,
                                             party
                                     ) a
                                 JOIN new_type.message b ON b.timesent = a.timesent
                                 AND a. USER = b. USER
                                 AND a.party = b.party
                                 GROUP BY
                                     a.uuid,
                                     a. USER,
                                     a.count,
                                     self_filter (b.content),
                                     a.timesent,
                                     b.partyuid,
                                     b.platform_msg_id,
                                     a.party,
                                     b.time,
                                     b.platform
                             ) d
                         WHERE
                             d.content != 'null'

                         """)
        hc.sql(""" INSERT INTO TABLE new_type.src_message_count PARTITION (time,platform) SELECT
                              t.uuid,
                              t.USER,
                              t.count,
                              t.content,
                              t.timesent,
                              t.platform_msg_id,
                              t.platform,
                              t.partyuid,
                              t.party,
                              u. NAME,
                              u.phone,
                              u.mail,
                              t.time,
                              t.platform
                          FROM
                              new_type.msg_count_tmp t
                          LEFT JOIN new_type.user u ON  u.rowkey = t.party

                          """)

    def total_message_count():
        hc.sql("""
            INSERT overwrite TABLE new_type.total_message_count PARTITION (time = '{time}') SELECT a.user,
            size (collect_set(a.mid)) 
            FROM ( SELECT
            x.user,
            x.mid
            FROM
            new_type.message x
                UNION ALL
                    SELECT
                        y.party user,
                        y.mid
                        FROM
                    new_type.message y) a
            GROUP BY
            a.user  """.format(time=TIME))

    src_message_count()
    total_message_count()


def group_message_count():
    def src_groupmsg_count():
        hc.sql("""
                    SELECT
                        concat(b.sender, '-', a.guid) uuid,
                        b.sender,
                        a.count,
                        b.content,
                        a.timesent,
                        b.media,
                        b.platform_msg_id,
                        b.platform,
                        a.guid
                        b.time
                    FROM
                        (
                            SELECT
                                concat(platform, '-', d.gid) guid,
                                count(1) count,
                                max(d.timesent) timesent,
                                d.gid
                                d.platform
                            FROM
                                new_type.group_message d
                            WHERE
                                d.msgtype = 1
                            GROUP BY
                                platform,
                                d.gid
                        ) a
                    JOIN new_type.group_message b ON b.timesent = a.timesent
                    AND a.gid = b.gid
                    AND b.platform = a.platform
                    """).registerTempTable("group_msg_count_tmp")

        # 关联group name
        hc.sql("""
            INSERT overwrite TABLE new_type.src_groupmsg_count PARTITION (time,platform) SELECT
                t.uuid,
                t.sender,
                t.count,
                t.content,
                t.timesent,
                t.media,
                t.guid,
                a.groupname
                t.time,
                t.platform
            FROM
                group_msg_count_tmp t
            JOIN new_type.group_info a ON a.groupuid = t.guid
            """)

    def total_groupmsg_count():
        hc.sql("""
                                INSERT overwrite TABLE new_type.total_groupmsg_count PARTITION (time = '{0}') SELECT
                                    concat(platform, '-', gid),
                                    count(1)
                                FROM
                                    new_type.group_message
                                GROUP BY
                                    gid,
                                    platform
                                """.format(TIME))
        src_groupmsg_count()
        total_groupmsg_count()


if __name__ == '__main__':
    conf = SparkConf().setAppName("spark_transform_data_py ")
    sc = SparkContext(conf=conf)
    hc = HiveContext(sc)
    phone_json = sc.broadcast(jsonData)
    TransformTango.execute_transform()
    TransformNimbuzz.execute_transform()
    TransformViber.execute_transform()
    TransformHike.execute_transform()
