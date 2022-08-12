from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Basics').getOrCreate()

df= spark.read.csv('sales_info-2.csv', inferSchema = True, header =True)
df.groupBy("Company").mean().show()
df.groupBy("Company").sum().show()
df.groupBy("Company").min().show()
df.groupBy("Company").max().show()
df.groupBy("Company","Sales").count().show()
