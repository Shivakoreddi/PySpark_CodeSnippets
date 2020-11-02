# -*- coding: utf-8 -*-
"""
author SK
Naming Standard = PySpakr_CodeSnippets as pcs

"""

import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')

import pyspark
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('Collect').getOrCreate()
dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) ]
deptColumns = ["deptName","dept_id"]
deptDF = spark.createDataFrame(data=dept,schema=deptColumns)
dataCollect = deptDF.collect()
for row in dataCollect:
    print(row['dept_name']+","+str(row['dept_id']))
