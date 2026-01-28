import pyspark,time,platform,sys,os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp
import pandas as pd
import matplotlib.pyplot as plt 
from sqlalchemy import inspect,create_engine
from pandas.io import sql
import warnings,matplotlib
warnings.filterwarnings("ignore")
t0=time.time()
con=create_engine("mysql://root:%40Alex444@localhost/test")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Hi").getOrCreate()
columns = ["id","category_id","rate","title","author"]
data = [("1", "1","5","java","author1"),
        ("2", "1","5","scala","author2"),
        ("3", "1","5","python","author3")]
if 1==11:
    df = spark.createDataFrame(data,columns)
    df.withColumn("id",col("id").cast("int"))\
        .withColumn("category_id",col("category_id").cast("int"))\
        .withColumn("rate",col("rate").cast("int"))\
        .withColumn("dt", current_timestamp())\
        .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=alex&password=1")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4a")\
        .mode("overwrite").save()
spark.stop()
t1=time.time()
print('finished',time.strftime('%H:%M:%S',time.gmtime(round(t1-t0))))
