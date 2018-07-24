#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : pureoym
# @Contact : pureoym@163.com
# @TIME    : 2018/7/24 9:27
# @File    : streaming_app.py
# Copyright 2017 pureoym. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ========================================================================
"""
 在线流式计算。每一个时间窗内，通过分析用户行为日志，与新闻池中的新闻数据，
 得到用户的关键词列表，并通过列表生成待推荐的新闻列表。将改列表更新至用户
 表中。
"""
from __future__ import print_function
from urllib2 import urlopen, URLError
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import re
import time
import logging
import db_utils
import json
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


def main(ssc, zkQuorum, topic):
    '''

    :param ssc:
    :param zkQuorum:
    :param topic:
    :return:
    '''
    # zkQuorum = '10.10.192.64:2181'
    # topic = 'test-topic'
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "newsrec-actions-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])

    lines.pprint()

    # user_actions = get_user_actions(lines)
    # user_actions2 = user_actions.window(60, 60).cache()
    # user_actions2.foreachRDD(lambda rdd: rdd.foreachPartition(save_user_actions))
    #
    # users = get_users(user_actions2)
    # users.pprint()
    # users.foreachRDD(lambda rdd: rdd.foreachPartition(save_users))


def get_user_actions(input):
    '''
    通过日志提取:user_actions rdd
    :param input:
    :return: user_actions rdd (time,uid,iid)
    '''
    f1 = lambda line: "]:VIEW_NEWS_PAGE" in line or \
                      ("[A]:HIT_PLACE" in line and "[V]:detail_suggest" in line)
    user_actions = input.filter(f1) \
        .map(get_actions_func) \
        .filter(lambda (action_time, uid, iid): uid != "" and iid != "")

    return user_actions


def get_actions_func(input):
    '''
    user_actions提取方法
    :param input_log:
    :return: user_actions rdd(time,uid,iid)
    '''
    action_time = input[:19]
    # action_type = ""
    uid = get_user_id_from_log(input)
    iid = get_item_id_from_log(input)
    return action_time, uid, iid


def get_user_id_from_log(input):
    '''
    通过正则获取日志中的user_id
    :param input:
    :return: user_id
    '''
    user_id = ""
    pattern1 = r"deviceid=[^( |&)]+[ |&]"
    pattern2 = r"udid:[^( |&)]+[ |&]"
    if re.search(pattern1, input):
        user_id = re.search(pattern1, input).group()[9:-1]
    if re.search(pattern2, input):
        user_id = re.search(pattern2, input).group()[5:-1]
    return user_id


def get_item_id_from_log(input):
    '''
    获取日志中的item_id
    :param input:
    :return: item_id
    '''
    item_id = ""
    if "[NEWS-URL]:" in input:
        url = input.split("[NEWS-URL]:")[1]
        if "detail" in url:
            item_id = url.split("/")[-1].split("_")[0]
    return item_id


def get_users(input):
    '''
    生成users rdd：(uid, iids, tags）
    :param input: user_actions rdd:(time,uid.iid)
    :return: users rdd：(uid, iids, tags）
    '''
    # 每5分钟算一次最近15分钟的数据
    users = input.map(lambda (action_time, uid, iid): (uid, iid)) \
        .window(60 * 15, 60 * 5) \
        .reduceByKey(lambda iid1, iid2: iid1 + u',' + iid2) \
        .map(lambda (uid, iids): (uid, iids, get_tags_from_api(iids)))
    return users


def save_user_actions(iter):
    '''
    分区提交mysql
    :param iter:
    :return:
    '''
    logger = logging.getLogger(__name__)
    conf = {'host': '10.10.65.231',
            'user': 'recommend',
            'passwd': 'recommend_123',
            'db': 'recommend',
            'port': 3306, }
    conn = db_utils.get_mysql_conn(conf)
    try:
        for record in iter:
            try:
                user_id = record[1]
                item_id = record[2]
                action_time = record[0]
                sql = 'REPLACE INTO news_rec_actions(user_id,item_id,action_time) VALUES ("{0}",{1},"{2}")' \
                    .format(user_id, item_id, action_time)
                db_utils.execute_mysql_sql(conn, sql)
            except Exception, e:
                logger.error(e)
                continue
    finally:
        if conn is not None:
            conn.close()


def save_users(iter):
    '''
    分区提交mysql
    :param iter:
    :return:
    '''
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    logger = logging.getLogger(__name__)
    conf = {'host': '10.10.65.231',
            'user': 'recommend',
            'passwd': 'recommend_123',
            'db': 'recommend',
            'port': 3306, }
    conn = db_utils.get_mysql_conn(conf)
    update_time = get_datetime()
    try:
        for record in iter:
            try:
                user_id = record[0]
                item_list = record[1]
                tags = record[2]
                sql = 'REPLACE INTO news_rec_users(user_id,item_list,tags,update_time) VALUES ("{0}","{1}","{2}","{3}")' \
                    .format(user_id, item_list, tags, update_time)
                db_utils.execute_mysql_sql(conn, sql)
            except Exception, e:
                logger.error(e)
                continue
    finally:
        if conn is not None:
            conn.close()


def get_datetime():
    '''
    获取当前时间
    :return:
    '''
    return time.strftime("%Y-%m-%d %H:%M:%S")


def get_tags_from_api(iids):
    '''
    通过接口获取tags
    :param item_list:
    :return:
    '''
    logger = logging.getLogger(__name__)
    if iids is None:
        return ''
    url = generate_request_url(iids)
    try:
        response = urlopen(url)
        response_data = response.read()
        tags = parse_tags_from_json(response_data)
        return tags
    except URLError, e:
        logger.error(e)
        return ''


def generate_request_url(iids):
    '''
    通过iids获取api请求url
    :param item_list:
    :return:
    '''
    return 'http://data.cms.mgt.chinaso365.com/srv/access/getByIds.json' \
           '?tableName=news&contentIds=' + iids + '&fields=Tag'


def parse_tags_from_json(input):
    '''
    解析json获取tags
    :param input:
    :return:
    '''
    j = json.loads(input)
    l1 = j['content']
    tag_list = []
    output = ''
    for l in l1:
        tags = l['Tag'].split(',')
        # 如果标签中超过5个词，则忽略
        if len(tags) < 6:
            tag_list.extend(tags)
    tag_dict = dict(map(lambda x: (x, 0), tag_list))
    for tag in tag_list:
        tag_dict[tag] = tag_dict[tag] + 1
    tag_list = sorted(tag_dict.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
    for tag in tag_list:
        # output += '{0}:{1},'.format(tag[0], tag[1])
        if tag[0] != '':  # 去除空标签
            output += unicode(tag[0]) + u':' + unicode(tag[1]) + u','
    return output[:-1]


if __name__ == "__main__":
    # if len(sys.argv) != 3:
    #     print("Usage: newsrec_app.py <zk> <topic>", file=sys.stderr)
    #     exit(-1)
    # logger = logging.getLogger(__name__)
    conf = SparkConf().setAppName("emergency detector app").setMaster("spark://10.10.160.151:7077")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)
    zkQuorum = "10.10.192.64:2181"
    topic = "weibo-topic"
    main(ssc, zkQuorum, topic)
    ssc.start()
    ssc.awaitTermination()
