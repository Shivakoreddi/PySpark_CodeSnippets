# -*- coding: utf-8 -*-
"""
author SK
Naming Standard = PySpakr_CodeSnippets as pcs

"""
import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')

import pyspark
from pyspark.sql import SparkSession
from pyspark import Row

spark = SparkSession.builder.appName('CreateDataFrame').getOrCreate()
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
rdd = spark.sparkContext.parallelize(data)
##Pyspark RDD's toDF() method

dfFromRDD = rdd.toDF()
dfFromRDD.printSchema()
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()

## Create DF from Row Type
rowData = map(lambda x:Row(*x),data)
dfFromData1 = spark.createDataFrame(rowData,schema=columns)


## Create DF from csv

df2 = spark.read.csv("birds.csv")
df2.show(truncate = False)
