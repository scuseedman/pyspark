#!/usr/bin/env python
# -*- coding:utf8 -*-

"""
@version: ??
@author: lwd
@license: Apache Licence 
@contact: scuseedman@163.com
@site: http://www.alaxigaodi.com
@software: PyCharm
@file: WordCountLocal.py
@time: 2017/12/5 10:28

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
from pyspark import SparkContext
os.environ["SPARK_HOME"] = "D:\soft\spark-1.6.0-bin-hadoop2.6"

"""
读取本地文件使用spark进行wordcount计算词频
"""
if __name__ == '__main__':
    sc = SparkContext(appName="wc local")
    counts = sc.textFile("words")
    datas = counts.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y).collect()
    print(datas)
    sc.stop()

