#!/usr/bin/python
# -*- coding:utf-8 -*-
# author :lwd
# date :20170522
# 来源于spark文档中的官方实例测试
from pyspark import SparkContext,SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
#from pyspark.sql.functions import col

sqlContext = conf = SparkConf().setAppName("the apache sparksql")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

l = [("zhangfei",1),("guanyu",33)]
row = sqlContext.createDataFrame(l).collect()
row = sqlContext.createDataFrame(l, ['name', 'age']).collect()
print(row)
d = [{"name":"zhangfei","age":33},{"name":"guanyu","age":44}]
row = sqlContext.createDataFrame(d).collect()
print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx ")
print(row)
rdd = sc.parallelize(l)
row = sqlContext.createDataFrame(rdd).collect()
print("xxxxxxxxxxxxxxxxxxxx the 2nd xxxxxxxxxxxxxxxxxxxxxxxxxxx")
print(row)

df = sqlContext.createDataFrame(rdd, ['name', 'age'])
row = df.collect()
print("xxxxxxxxxxxxxxxxxxxx the 3rd xxxxxxxxxxxxxxxxxxxxxxxxxxx")
print(row)
# 注意这里两个变量 Person和person
Person = Row("name","age")
person = rdd.map(lambda r:Person(*r))
res1 = sqlContext.createDataFrame(person)
row = res1.collect()
print("xxxxxxxxxxxxxxxxxxxx the 4th xxxxxxxxxxxxxxxxxxxxxxxxxxx")
print(row)
# ----------------------------------------5 th
schema = StructType([ StructField("name", StringType(), True), StructField("age", IntegerType(), True)])
res3 = sqlContext.createDataFrame(rdd, schema)
row = res3.collect()
print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx 5th xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
print(row)
# -------------------------------------------- 现在该示例未能执行成功，错误消息为：ImportError: No module named pandas
#row = sqlContext.createDataFrame(df.toPandas()).collect()  
#print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx 6th xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
#print(row)
# -------------------------------------------- createExternalTable(tableName, path=None, source=None, schema=None, **options) and drop it 
print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx 7th xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
sqlContext.registerDataFrameAsTable(df, "stu5")
sqlContext.dropTempTable("stu5")
# ---------------------------------------------- json file 待测
# ---------------------------------------------- 
print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx 8th xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
sqlContext.registerDataFrameAsTable(df, "stu6")
df2 = sqlContext.sql("SELECT name AS f1, age as f2 from stu6")
print(df2.collect())
sqlContext.table("stu6")
print(sorted(df.collect()) == sorted(df2.collect()))    # True
# -------------------------------------------------------
print("stu6" in sqlContext.tableNames())    #True
print("stu3" in sqlContext.tableNames())    #False
# -------------------------------------------------------
sqlContext.registerDataFrameAsTable(df, "stu5")
df2 = sqlContext.tables()
print(df2.filter("tableName = 'stu5'").first())     # Row(tableName=u'stu5', isTemporary=True)
# -------------------------------------------------------
row = df.agg({"age":"max"}).collect()
print(row)
row = df.agg(F.min(df.age)).collect()
print(".................................................... the other function 2 collect .......................")
print(row)
# -------------------------------------------------------
df_as1 = df.alias("df_as1")
df_as2 = df.alias("df_as2")
joined_df = df_as1.join(df_as2, col("df_as1.name") == col("df_as2.name"), 'inner')
row = joined_df.select(col("df_as1.name"), col("df_as2.name"), col("df_as2.age")).collect()
print(".................................................... the other function 2 collect .......................")
print(row)
# -------------------------------------------------------
row = df.coalesce(1).rdd.getNumPartitions()
print(row)
print(df.collect())
print(df.columns)
print(df.count())
# -------------------------------------------------------
print("....................................................  .......................")
row = df.cube('name', df.age).count().show()
print("....................................................  .......................")
df.describe().show()
print("....................................................  .......................")
df.describe(['age', 'name']).show()
print("....................................................  .......................")
df.distinct().count()
print(".................................................... df.drop .......................")
row = df.drop('age').collect()
print(row)
#[Row(name=u'zhangfei'), Row(name=u'guanyu')]
row = df.drop(df.age).collect()
print(row)
#[Row(name=u'zhangfei'), Row(name=u'guanyu')]
df2 = sqlContext.sql("SELECT name , age  from stu6")
row = df.join(df2, df.name == df2.name, 'inner').drop(df.name).collect()
print(row)
#[Row(age=1, name=u'zhangfei', age=1), Row(age=33, name=u'guanyu', age=33)]      
row = df.join(df2, df.name == df2.name, 'inner').drop(df2.name).collect()
print(row)
#[Row(name=u'zhangfei', age=1, name=u'zhangfei', age=1), Row(name=u'guanyu', age=33, name=u'guanyu', age=33)]
# -------------------------------------------------------
df = sc.parallelize([Row(name='Alice',age=5,height=80),Row(name='Alice',age=5,height=80),Row(name='Alice',age=10,height=80)]).toDF()
df2 = sc.parallelize([Row(name='Alice',age=15,height=80),Row(name='guanyu',age=25,height=80),Row(name='liubei',age=56,height=80)]).toDF()
print(".................................................... df.dropDuplicates().show() .......................")
df.dropDuplicates().show()
print(".................................................... df.dropDuplicates(['name', 'height']).show() .......................")
df.dropDuplicates(['name', 'height']).show()
# -------------------------------------------------------
df.na.drop().show()
print(df.dtypes)
# -------------------------------------------------------
print(".................................................... df.explain()  .......................")
print(df.explain())
# -------------------------------------------------------
print(".................................................... df.explain()  .......................")
df.na.fill(50).show()
df.na.fill({'age': 50, 'name': 'unknown'}).show()
# -------------------------------------------------------
print(".................................................... df.filter()  .......................")
row = df.filter(df.age>5).collect()
print(row)
row = df.filter("age>5").collect()
print(row)
# -------------------------------------------------------
print(".................................................... df.where()  .......................")
row = df.where(df.age==10).collect()
print(row)
row = df.where("age = 10").collect()
print(row)
print(df.first())
# -------------------------------------------------------
print(".................................................... df.flatmap()  .......................")
row = df.flatMap(lambda p: p.name).collect()
print(row)
# -------------------------------------------------------
print(".................................................... df.groupby()  .......................")
row = df.groupBy().avg().collect()
print(row)
row = df.groupBy('name').agg({'age': 'mean'}).collect()
print(row)
row = df.groupBy(df.name).avg().collect()
print(row)
row = df.groupBy(['name', df.age]).count().collect()        #group by 2 columns ??
print(row)
# -------------------------------------------------------
print(".................................................... df.head()  .......................")
print(df.head())
print(df.head(1))
# -------------------------------------------------------
print(".................................................... df join  .......................")
row = df.join(df2, df.name == df2.name, 'outer').select(df.name, df2.height).collect()
print(row)
# -------------------------------------------------------
print(".................................................... df.printSchema()  .......................")
df.printSchema()
# -------------------------------------------------------
print(".................................................... df.registerTempTable()  .......................")
df.registerTempTable("people")
df2 = sqlContext.sql("select * from people")
print(sorted(df.collect()) == sorted(df2.collect()))
# -------------------------------------------------------
print(".................................................... df.registerTempTable()  .......................")
dataset = sqlContext.range(0, 100).select((col("id") % 3).alias("key"))
sampled = dataset.sampleBy("key", fractions={0: 0.1, 1: 0.2}, seed=0)
sampled.groupBy("key").count().orderBy("key").show()
# -------------------------------------------------------
print(".................................................... df.select(columns)  .......................")
df.select('*').collect()
df.select('name', 'age').collect()
row = df.select(df.name, (df.age + 10).alias('age')).collect()
print(".................................................... df.selectExpr(expr)  .......................")
row = df.selectExpr("age * 2", "abs(age)").collect();
print(row)
df.show()
print("...........................................................................")
