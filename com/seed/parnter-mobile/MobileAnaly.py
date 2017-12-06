#!/usr/bin/env python
# -*- coding:utf8 -*-

"""
@version: ??
@author: lwd
@license: Apache Licence 
@contact: scuseedman@163.com
@site: http://www.alaxigaodi.com
@software: PyCharm
@file: MobileAnaly.py
@time: 2017/12/6 10:24

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


#处理partner文件
def partneranaly(partnerfile):
    partners = sc.textFile(partnerfile)
    rows = partners.map(lambda line:line.split("|"))
        # .map(lambda word: Row(partner_no=word[0], city_name=word[1], area_name=word[2],sc_pid=word[3],sc_id=word[4],comp_addr=word[5]))  # 不能这样处理，加了这个数据格式会错乱
    fields = []
    fields.append(StructField("partner_no", StringType(), True))
    fields.append(StructField("city_name", StringType(), True))
    fields.append(StructField("area_name", StringType(), True))
    fields.append(StructField("sc_pid", StringType(), True))
    fields.append(StructField("sc_id", StringType(), True))
    fields.append(StructField("comp_addr", StringType(), True))
    schema = StructType(fields)

    df = sqlContext.createDataFrame(rows, schema)
    print(df.show())
    print("................ 萌萌的分割线 ..........................")
    return df


#处理mobile
def mobileanaly(mobilefile):
    mobiles = sc.textFile(mobilefile)
    rows = mobiles.map(lambda line: line.split("\t"))
    fields = []
    fields.append(StructField("partner_no", StringType(), True))
    fields.append(StructField("comp_no", StringType(), True))
    fields.append(StructField("mac_addr", StringType(), True))
    fields.append(StructField("mobile", StringType(), True))
    schema = StructType(fields)

    df = sqlContext.createDataFrame(rows, schema)
    print(df.show())
    print("................ 萌萌的分割线 ..........................")
    return df


def mobileleftjoinpartner(p_df,m_df):
    joined_df = m_df.join(p_df, m_df.partner_no == p_df.partner_no, 'left_outer')
    rows = joined_df.select(m_df.mobile,p_df.partner_no,p_df.city_name,p_df.area_name,p_df.comp_addr)
    print(rows.columns)
    # for x in rows.collect:
    #     print(x)
    print(rows.show())
    return rows


# 数据结果处理，待看文档
def savedfastext(rows):
    print("........................... 数据结果格式化")
    print(rows.columns)
    print(rows.describe(["city_name","mobile"]).show())
    # rows.select("mobile").write.format("text").mode("append").save("joinres.txt")
    # rows.write.save("namesAndAges.parquet", format="parquet")       #存储为目录下的文件，格式为parquet
    prop = {"user":"root","password":"hadoop","driver":"com.mysql.jdbc.Driver"}
    rows.write.format("jdbc").jdbc("jdbc:mysql://hadoop03:3306/test","t_spark_dataframe_test",prop) # // 表可以不存在,数据库连接失败 seed

"""
The solution is to add an environment variable named as "PYSPARK_SUBMIT_ARGS" and set its value to
"--packages com.databricks:spark-csv_2.10:1.4.0 pyspark-shell".It will work fine.
"""

if __name__ == '__main__':
    sqlContext = conf = SparkConf().setAppName("log analy")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.setLogLevel("WARN")
    partnerfile = "../test/partner_city_area"
    partner_df = partneranaly(partnerfile)

    mobilefile = "../test/mobile_demo.txt"
    mobile_df = mobileanaly(mobilefile)
    print("....................... 表开始关联 .........................")
    rows = mobileleftjoinpartner(partner_df, mobile_df)
    savedfastext(rows)
    sc.stop()
