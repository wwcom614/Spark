#!/usr/bin/python
# -*- coding: UTF-8 -*-

import random
from random import randint
import time

__author__ = "wwcom123"

# 【数据流--1】：数据源

# 为后面统计结果观察验证方便，只生成某个月，某几天的数据
month = ["DEC"]
day = list(range(24, 27, 1))
hour = list(range(10, 24, 1))
minute_second =  list(range(10, 60, 1))

# 4位IPV4地址范围
ip_slices = list(range(2, 254, 1))

http_method = ["POST" ,"GET"]

# 模拟
url_path = [
    "video/1001",
    "video/1002",
    "video/1003",
    "video/1004",
    "video/1005",
    "video/1006",
    "audio/1001",
    "audio/1002",
    "book/1001",
    "book/1002",
    "book/1003"
]

# 模拟返回HTTP状态码
http_status = ["200", "404", "500"]

# 模拟访问流量
traffic = list(range(2000, 8000, 1))


# join() 方法用于将序列中的元素以.join()左侧指定的字符连接生成一个新的字符串
#随机生成访问源IP
def sample_ip():
    ip_slice_sample4 = random.sample(ip_slices, 4)
    return ".".join(str(x) for x in ip_slice_sample4)

#模拟随机生成 [11/DEC/2018:03:04:05 +0800]格式的时间日志
def sample_time():
    min_sec = random.sample(minute_second,2)
    min_sec_join = str(":".join(str(x) for x in min_sec))
    hour_join = str(random.sample(hour, 1)[0])
    day_join = str(random.sample(day ,1)[0])
    month_join = random.sample(month ,1)[0]
    return "[" + day_join + "/" + month_join + "/2018 " + hour_join + ":" + min_sec_join + " +0800]"

#模拟消耗流量字节数，放回采样
def sample_traffic():
    return str(random.sample(traffic,1)[0])

# sample：从序列a中随机抽取n个元素，并将n个元素生以list形式返回
#随机生成用户访问的URL
def sample_url():
    return random.sample(url_path, 1)[0]

# 模拟生成用户访问网址URL记录字段
def sample_url_click():
    return "\"http://www.videonet.com/"+sample_url()+"\""

#模拟随机生成HTTP状态码，放回采样
def sample_status():
    return random.sample(http_status, 1)[0]

#模拟生成"POST video/006 HTTP1.0"这3个字段的日志
def sample_method_url_status():
    return "\""+ random.sample(http_method, 1)[0] + "\t" + sample_url() + "\tHTTP1.0\""


# 随机生成日志记录，数量可配置，不输入入参，默认生成10条
def genNetAccesslog(count = 10, path="OriginalData.txt"):
    f = open(path, "w+")
    while count >= 1:
        query_log = "{ip}\t-\t-\t{local_time}\t{method_url_status}\t{http_status}\t{traffic}\t{url_click}".format(ip=sample_ip(),local_time=sample_time(),method_url_status=sample_method_url_status(),http_status=sample_status(),traffic=sample_traffic(),url_click=sample_url_click())
        print(query_log)
        f.write(query_log+"\n")
        count = count - 1
    f.close()

if __name__ == '__main__':
    genNetAccesslog(1000)
    # print(sample_time())