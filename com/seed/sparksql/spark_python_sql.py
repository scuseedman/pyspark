#!/usr/bin/python
# -*- encoding:utf-8 -*-

"""
@author: xuanyu
@contact: xuanyu@126.com
@file:.py
@time:2017/5/16 21:54
"""

# 导入模块 pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
# 导入系统模块
import os
import time

if __name__ == '__main__':

    # 设置SPARK_HOME环境变量
    os.environ['SPARK_HOME'] = 'E:/spark-1.6.1-bin-2.5.0-cdh5.3.6'
    os.environ['HADOOP_HOME'] = 'G:/OnlinePySparkCourse/pyspark-project/winuntil'

    # Create SparkConf
    sparkConf = SparkConf()\
        .setAppName('Python Spark WordCount')\
        .setMaster('local[2]')

    # Create SparkContext
    sc = SparkContext(conf=sparkConf)
    # 设置日志级别
    sc.setLogLevel('WARN')

    # Create SQLContext
    sqlContext = SQLContext(sparkContext=sc)

    """
        Taobao
            HDFS：
                /user/hive/warehouse/page_views
            Data Structure:
                track_time              string
                url                     string
                session_id              string
                referer                 string
                ip                      string
                end_user_id             string
                city_id                 string
            每行数据字段之间使用制表符进行分割
    """
    # transform function
    def map_func(line):
        # split line
        arr = line.split("\t")
        # return
        return Row(track_time=arr[0], url=arr[1], session_id=arr[2], referer=arr[3],
                   ip=arr[4], end_user_id=arr[5], city_id=arr[6])

    # read data from hdfs and transform RDD[Row]
    page_views_rdd = sc\
        .textFile("/user/hive/warehouse/page_views")\
        .map(map_func)

    # Create DataFrame
    page_views_df = sqlContext.createDataFrame(page_views_rdd)

    # # print
    # print page_views_df.count()
    # page_views_df.show()

    """
        基于SQL进行数据分析
    """
    # Register Temp Table
    page_views_df.registerTempTable("tmp_page_views")

    # 需求：按照session_id进行分组，统计次数，会话PV
    session_pv = sqlContext.sql("""
        SELECT
            session_id, COUNT(1) AS cnt
        FROM
            tmp_page_views
        GROUP BY
            session_id
        ORDER BY
            cnt DESC
        LIMIT
            10
    """).map(lambda output: output.session_id + "\t" + str(output.cnt))
    for result in session_pv.collect():
        print result

    """
        DSL 数据分析
    """
    session_count = (
        page_views_df
            .groupBy("session_id")
            .count()
            .sort("count", ascending=False)
            .limit(10)
    ).map(lambda output: output.session_id + "\t" + str(output['count']))
    for result in session_count.collect():
        print result

    # WEB UI 4040, 让线程休眠一段时间
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()
