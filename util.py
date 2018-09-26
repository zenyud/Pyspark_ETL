#!/usr/bin/env python
# coding: utf-8
# @Time    : 2018/8/22 11:06
# @Author  : zeng yu 
# @Site    : 
# @File    : util.py
# @Software: PyCharm

from xml.parsers.expat import ParserCreate
import json
import os


class Xml2Json:
    LIST_TAGS = ['COMMANDS']

    def __init__(self, data=None):
        self._parser = ParserCreate()
        self._parser.StartElementHandler = self.start
        self._parser.EndElementHandler = self.end
        self._parser.CharacterDataHandler = self.data
        self.result = None
        if data:
            self.feed(data)
            self.close()

    def feed(self, data):
        self._stack = []
        self._data = ''
        self._parser.Parse(data, 0)

    def close(self):
        self._parser.Parse("", 1)
        del self._parser

    def start(self, tag, attrs):
        assert attrs == {}
        assert self._data.strip() == ''
        self._stack.append([tag])
        self._data = ''

    def end(self, tag):
        last_tag = self._stack.pop()
        assert last_tag[0] == tag
        if len(last_tag) == 1:  # leaf
            data = self._data
        else:
            if tag not in Xml2Json.LIST_TAGS:
                # build a dict, repeating pairs get pushed into lists
                data = {}
                for k, v in last_tag[1:]:
                    if k not in data:
                        data[k] = v
                    else:
                        el = data[k]
                        if type(el) is not list:
                            data[k] = [el, v]
                        else:
                            el.append(v)
            else:  # force into a list
                data = [{k: v} for k, v in last_tag[1:]]
        if self._stack:
            self._stack[-1].append((tag, data))
        else:
            self.result = {tag: data}
        self._data = ''

    def data(self, data):
        self._data = data


def get_hbase_params(file, params):
    res = {}
    if not os.path.isfile(file):
        if os.path.isfile("d:/spark-1.6.0-bin-hadoop2.6/conf/hbase-site.xml"):
            file = "d:/spark-1.6.0-bin-hadoop2.6/conf/hbase-site.xml"
        else:
            return res
    xml = open(file, 'r').read()
    result = Xml2Json(xml).result
    if result:
        res_dict = {}
        properties = result['configuration']['property']
        for x in properties:
            res_dict[x['name']] = x
        for param in params:
            if res_dict.has_key(param):
                res.update({param: res_dict[param]})
            else:
                res.update({param: {'name': param, 'value': ''}})
    return res


def get_catlog(*colsnames, **tableInfo):
    org_catelog = {"table": {"namespace": "default", "name": "%s" % tableInfo.get("table_name")},
                   "rowkey": "%s" % tableInfo.get("rowkey"), "domain": "",
                   "columns": {"%s" % tableInfo.get("rowkey"): {"cf": "rowkey", "col": "key", "type": "string"}}}
    cols = {}
    for colname in colsnames:
        if colname != 'rowkey':
            cols[colname] = {"cf": "%s" % tableInfo.get("family"), "col": "%s" % colname.upper(), "type": "string"}

    cols.update(org_catelog.get("columns"))
    org_catelog["columns"] = cols
    return org_catelog
