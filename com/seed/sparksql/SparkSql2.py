#!/usr/bin/env python
# -*- coding:utf8 -*-

"""
@version: ??
@author: lwd
@license: Apache Licence 
@contact: scuseedman@163.com
@site: http://www.alaxigaodi.com
@software: PyCharm
@file: SparkSql.py
@time: 2017/12/5 10:50

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
from pyspark.sql import HiveContext,SQLContext,Row
from pyspark.sql.types import StructField,StructType,IntegerType,StringType

os.environ["SPARK_HOME"] = "D:\soft\spark-1.6.0-bin-hadoop2.6"
# ImportError: cannot import name StructField
# 此脚本目前还不能正确执行，需要特别注意,原因还未查找出来seed 程序可以执行 20171205 seed
conf = SparkConf().setAppName("The second SparkSQL")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
hc = HiveContext(sc)
# -------------------------------- （2）通过编码指定Schema ----------------------------- 2 start
# 使用反射推断Schema的方式要求我们必须能够构建一个数据类型为Row的Spark RDD，然后再将其转换为SchemaRDD；某些情况下我们可能需要更为灵>活的方式控制SchemaRDD构建过程，这正是通过编码指定Schema的意义所在。
# 通过编码指定Schema分为三步：
# a. 构建一个数据类型为tuple或list的Spark RDD；
# b. 构建Schema，需要匹配a中的tuple或list；
# c.将b中的Schema应用于a中的Spark RDD。
print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx  执行第二种方式 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
datas = ["1 zhangfei 33","2 liubei 56","3 guanyu 30"]
# b. 对datas执行“parallelize”操作，将其转换为Spark RDD source，数据类型为字符串；
source = sc.parallelize(datas)
# c. 将Spark RDD source中的每一条数据进行切片（split）后转换为Spark RDD rows，数据类型为Row；
# 至此Spark RDD rows已经具备转换为SchemaRDD的条件：它的数据类型为Row。
splits = source.map(lambda line:line.split(" "))
rows = splits.map(lambda words:(int(words[0]),words[1],int(words[2])))
fields = []
fields.append(StructField("id",IntegerType(),True))
fields.append(StructField("name",StringType(),True))
fields.append(StructField("age",IntegerType(),True))
schema = StructType(fields)
people = hc.applySchema(rows,schema)
people.registerTempTable("people")
results = hc.sql("select * from people where age>30 and age<56").collect()
print("...............................")
print(results)
print("...............................")
for res in results:
    print("id:%d,name:%s,age:%d" %(res.id,res.name,res.age))
sc.stop()
print("the program end ......")