# ascending and descending order
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark=SparkSession.builder.appName("basic").getOrCreate()

df=spark.read.csv('sales_info-2.csv', inferSchema=True,header=True)
#df.orderBy("Sales").show()
#arrenging it by ascending or descending format
#df.orderBy(df["Sales"].asc()).show()
#df.orderBy(df["Person"].asc()).show()

#syntax 3 for asc and decs
df.orderBy(col("Sales").asc()).show()


#syntax 4 for asc and decs

df.orderBy(df["Sales"].desc()).show()