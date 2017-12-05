#!/usr/bin/env python
# -*- coding:utf8 -*-

"""
@version: ??
@author: lwd
@license: Apache Licence 
@contact: scuseedman@163.com
@site: http://www.alaxigaodi.com
@software: PyCharm
@file: WordCountHdfs.py
@time: 2017/12/5 10:34

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
from pyspark import  SparkContext
import time
os.environ["SPARK_HOME"] = "D:\soft\spark-1.6.0-bin-hadoop2.6"

"""
读取HDFS文件进行spark计算词频，并将结果输出到HDFS
"""
def analyHdfsBySpark():
    sc = SparkContext(appName="wc hdfs")
    counts = sc.textFile("hdfs://hadoop01:8020/data/output/stu/stu.txt")
    datas = counts.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y)
    sc.stop()
    return datas


if __name__ == '__main__':
    datas = analyHdfsBySpark()
    print(datas.collect())
    n_date = time.strftime('%Y%m%d', time.localtime(time.time()))
    n_time = time.strftime('%H%M%S', time.localtime(time.time()))
    datas.saveAsTextFile("hdfs://hadoop01:8020/data/output/%s_%s" %(n_date,n_time))
