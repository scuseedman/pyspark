#!/usr/bin/env python
# -*- coding:utf8 -*-

"""
@version: ??
@author: lwd
@license: Apache Licence 
@contact: scuseedman@163.com
@site: http://www.alaxigaodi.com
@software: PyCharm
@file: Test.py
@time: 2017/12/5 16:00

# code is far away from bugs with the god animal protecting
    I love animals. They taste delicious.
              ┏┓      ┏┓
            ┏┛┻━━━┛┻┓
            ┃      ☃      ┃
            ┃  ┳┛  ┗┳  ┃
            ┃      ┻      ┃
            ┗━┓      ┏━┛
                ┃      ┗━━━┓
                ┃  神兽无影    ┣┓
                ┃　BUG无踪！   ┏┛
                ┗┓┓┏━┳┓┏┛
                  ┃┫┫  ┃┫┫
                  ┗┻┛  ┗┻┛
"""
import os
from pyspark import SparkContext,SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
os.environ["SPARK_HOME"] = "D:\soft\spark-1.6.0-bin-hadoop2.6"
if __name__ == '__main__':
    # line = "], EXCEPTION_TYPES=[java.io.InterruptedIOException], ROLE=[hdfs-DATANODE-c57918d4056e2e8ed6702e12abeec8c7seedman], SEVERITY=[INFORMATIONAL], SERVICE=[hdfs], HOST_IDS=[6cd8dc04-a9c1-402d-91b6-965bb54ec756], LOG_LEVEL=[INFO], ROLE_TYPE=[DATANODE], CATEGORY=[LOG_MESSAGE], SERVICE_TYPE=[HDFS], HOSTS=[hadoop03], EVENTCODE=[EV_LOG_EVENT]}, content=Exception for BP-1909682041-10.1.2.126-1470896701758:blk_1075727267_1986512, timestamp=1489541959750} - 1 of 60 failure(s) in last 57588s"
    # print(line)
    # print(("WARN" in line) or ("INFO" in line) or ("ERROR" in line))
    line = "0559a0031|黄山|屯溪区|3410|341002|安徽省黄山市屯溪区前园南路9号"
    rows = line.split("|")
    print(rows[0])
    print(rows[1])
    print(rows[2])
    print(rows[3])
    print(rows[4])
    print(rows[5])

