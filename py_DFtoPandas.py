import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')

import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType


spark = SparkSession.builder.appName('DFtoPandas').getOrCreate()
data = [("James","","Smith","36636","M",60000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data= data,schema=columns)
pysparkDF.printSchema()
pysparkDF.show(truncate=False)

##convert DF to Pandas

pandasDF = pysparkDF.toPandas()
print(pandasDF)

## Nested struct DataFrame

dataStruct = [(("James","","Smith"),"36636","M","3000"), \
      (("Michael","Rose",""),"40288","M","4000"), \
      (("Robert","","Williams"),"42114","M","4000"), \
      (("Maria","Anne","Jones"),"39192","F","4000"), \
      (("Jen","Mary","Brown"),"","F","-1") \
]

schemaStruct = StructType([
        StructField('name',StructType([
            StructField('fieldname',StringType(),True),
            StructField('middlename',StringType(),True),
            StructField('lastname',StringType(),True)
        ])),
        StructField('dob',StringType(),True),
        StructField('gender',StringType(),True),
        StructField('salary',StringType(),True)
        ])
df = spark.createDataFrame(data=dataStruct,schema=schemaStruct)
df.printSchema()
pandasDF2 = df.toPandas()
print(pandasDF2)
