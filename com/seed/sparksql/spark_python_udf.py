#!/usr/bin/python
# -*- encoding:utf-8 -*-

"""
@author: xuanyu
@contact: xuanyu@126.com
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
        definition function
    """
    # definition function
    def squared_func(number):
        return number * number
    # register function
    sqlContext.udf.register('func_squared', squared_func)

    # 指定返回类型
    from pyspark.sql.types import LongType
    sqlContext.udf.register('func_squared_type', squared_func, returnType=LongType())

    # 直接使用lambda
    sqlContext.udf.register('func_squared_lambda', lambda number: number * number)

    """
        Call the UDF in SparkSQL
    """
    # dataframe
    sqlContext.range(1, 20).registerTempTable("tmp_test")
    # SQL
    squared_result = sqlContext.sql("""
        SELECT id, func_squared(id) AS id_squared FROM tmp_test
    """).collect()
    for result in squared_result:
        print str(result['id']) + '\t' + str(result['id_squared'])

    """
        Use UDF with DataFrame
    """
    # 读取表中的数据
    test_df = sqlContext.table("tmp_test")
    from pyspark.sql.functions import udf
    squared_udf = udf(squared_func, LongType())
    #
    result_df = test_df.select('id', squared_udf('id').alias('id_squared'))
    #
    result_df.show()

    # WEB UI 4040, 让线程休眠一段时间
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()
