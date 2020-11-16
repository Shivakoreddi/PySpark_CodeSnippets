import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('CreateRDD').getOrCreate()
dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) ]
rdd = spark.sparkContext.parallelize(dept)
rdd.collect()
