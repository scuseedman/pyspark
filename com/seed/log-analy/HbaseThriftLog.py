#!/usr/bin/env python
# -*- coding:utf8 -*-

"""
@version: ??
@author: lwd
@license: Apache Licence 
@contact: scuseedman@163.com
@site: http://www.alaxigaodi.com
@software: PyCharm
@file: HbaseThriftLog.py
@time: 2017/12/5 14:23

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
import sys
from pyspark import SparkContext,SparkConf
from pyspark.sql import HiveContext,Row,SQLContext
from pyspark.sql.types import StructField,StructType,IntegerType,StringType
import time

os.environ["SPARK_HOME"] = "D:\soft\spark-1.6.0-bin-hadoop2.6"


def thriftloganaly():
    logs = sc.textFile("E:\chrome_download\hbase-cmf-hbase-HBASETHRIFTSERVER-test95.eformax.com.log\*.log*")
    # counts = logs.filter(lambda line:line.split(" ")[2] in ["INFO","WARN","ERROR","FATAL"])  #对数据进行过滤，将指定级别的行筛选出来
    # for y in counts.collect():
    #     print(y)
    # sys.exit(-1)
    # 执行必要的过滤条件，保证数据格式标准化
    datas = logs.filter(lambda line:len(line.split(" "))>2).filter(lambda line:line.split(" ")[2] in ["INFO","WARN","ERROR","FATAL"]).map(lambda line:line.split(" "))
    rows = datas.map(lambda word:Row(n_date=word[0],n_time=word[1],s_level=word[2])) #数据格式 Row(level=u'WARN', n_date=u'2017-11-11', n_time=u'19:27:16,653')
    print("开始呈现计算结果 ...... " )
    # for x in rows.collect():
    #     print(x)
    return rows


def excutesparksql(rows):
    fields = []
    fields.append(StructField("n_date",StringType(),True))
    fields.append(StructField("n_time",StringType(),True))
    fields.append(StructField("s_level",StringType(),True))
    schema = StructType(fields)
    logs = hc.applySchema(rows,schema)
    logs.registerTempTable("thrift_log")
    res = hc.sql("select s_level,count(1) times from thrift_log  group by s_level").collect()
    for result in res:
        print("level:%s,times:%d" % (result.s_level,result.times))
    return res


def writeresfile(res):
    print("")

if __name__ == '__main__':
    time.clock()
    conf = SparkConf().setAppName("The second SparkSQL")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    hc = HiveContext(sc)
    sc.setLogLevel("WARN")
    print("..............")
    rows = thriftloganaly()
    res = excutesparksql(rows)
    writeresfile(res)
    sc.stop()
    print("耗时： %f" %(time.clock()))