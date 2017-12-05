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
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext,Row
from pyspark.sql.types import StructField,StructType,IntegerType,StringType

os.environ["SPARK_HOME"] = "D:\soft\spark-1.6.0-bin-hadoop2.6"


if __name__ == '__main__':
    print("......... hello main method ......")
    # 使用反射推断schema
    conf = SparkConf().setAppName("1st sql in spark")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    hc = HiveContext(sc)
    # 创建一个数据集合，用于模拟数据源
    datas = ["1 zhangfei 44","2 guanyu 55","3 zilong 60"]
    # 对datas执行parallelize操作，将其转化为spark rdd source 数据类型为字符串
    source = sc.parallelize(datas)
    # c. 将spark rdd source中的每一条数据进行切片（split）后转换为spark rdd rows，数据类型为row；
    # 至此spark rdd rows已经具备转换为schemardd的条件：它的数据类型为row。
    splits = source.map(lambda line:line.split(" "))
    rows = splits.map(lambda words:Row(id = words[0],name=words[1],age=words[2]))
    # d. 使用HiveContext推断rows的schema，将其转换为schemardd people；
    people = hc.inferSchema(rows)
    # 通过people.printSchema()，我们可以查看推断schema的结果：
    people.printSchema()
    print("the first print end now ......")
    # e. 将schemardd people注册为一张临时表“people”；
    people.registerTempTable("people")
    #执行查询语句 select * from people where age>50 and age < 60 并将查询结果保存至spark rdd results，通过results.printSchema()的输出结果：
    res = hc.sql("select * from people where age>50 and age<60")
    res.printSchema()
    print(".......................................................... 萌萌的")
    # schemardd results2的数据类型为row，受到查询语句（select name）的影响，其仅包含一列数据，列名为name。
    res1 = hc.sql("select name from people")
    res1.printSchema()
    print(".......................................................... 萌萌的")
    # 可以执行表关联操作
    res2 = hc.sql("select a.name,a.age,b.id,b.name from people a join people b on a.id=b.id")
    res2.printSchema()
    # h. schemardd也可以执行所有spark rdd的操作，这里我们通过map将results2中的name值转换为大写形式，最终的输出结果：
    print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx 再一次进入分割线 xxxxxxxxxxxxxxxxxxxxxx")
    res3 = res1.map(lambda row:row.name.upper()).collect()
    print(res3)
    # 上述示例说明以下三点：
    # a. 我们可以将一个数据类型为row的spark rdd转换为一个schemardd；
    # b. schemardd可以注册为一张临时表执行sql查询语句，其查询结果也是一个schemardd；
    # c. schemardd可以执行所有spark rdd的操作。
    # -------------------------------- （1）使用反射推断schema -----------------------------1 end
    print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

    # -------------------------------- （2）通过编码指定schema ----------------------------- 2 start
    # 使用反射推断schema的方式要求我们必须能够构建一个数据类型为row的spark rdd，然后再将其转换为schemardd；某些情况下我们可能需要更为灵>活的方式控制schemardd构建过程，这正是通过编码指定schema的意义所在。
    # 通过编码指定schema分为三步：
    # a. 构建一个数据类型为tuple或list的spark rdd；
    # b. 构建schema，需要匹配a中的tuple或list；
    # c.将b中的schema应用于a中的spark rdd

    datas = ["1 zhangfei 33","2 liubei 56","3 guanyu 30"]
    ## b. 对datas执行“parallelize”操作，将其转换为spark rdd source，数据类型为字符串；
    source = sc.parallelize(datas)
    ## c. 将spark rdd source中的每一条数据进行切片（split）后转换为spark rdd rows，数据类型为row；
    ## 至此spark rdd rows已经具备转换为schemardd的条件：它的数据类型为row。
    splits = source.map(lambda line:line.split(" "))
    rows = splits.map(lambda words:row(id=words[0],name=words[1],age=words[2]))
    fields = []
    fields.append(StructField("id",IntegerType(),True))
    fields.append(StructField("name",StringType(),True))
    fields.append(StructField("age",IntegerType(),True))
    schema = StructType(fields)
    people2 = hc.applySchema(rows,schema)
    people2.registerTempTable("people2")
    results = hc.sql("select * from people2 where age>50 and age < 60").collect()
    sc.stop()
    print(results)
    for res in results:
        print("id:%s,name:%s,age:%s" %(result.id,result.name,result.age))



